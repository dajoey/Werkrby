"""Draw the Werkrby icon: a 32x32 pixel-art worker bee in a hard hat."""
import sys
from pathlib import Path
from PIL import Image

N = 32
OUT = Path(sys.argv[1])

PAL = {
    "outline": (34, 22, 38, 255),
    "yellow": (255, 200, 46, 255),
    "yellow_hi": (255, 234, 128, 255),
    "yellow_lo": (222, 142, 22, 255),
    "black": (48, 36, 58, 255),
    "black_hi": (84, 68, 100, 255),
    "wing": (196, 232, 255, 255),
    "wing_hi": (232, 248, 255, 255),
    "wing_lo": (140, 196, 232, 255),
    "hat": (255, 140, 26, 255),
    "hat_hi": (255, 196, 92, 255),
    "hat_lo": (196, 82, 14, 255),
    "white": (255, 255, 255, 255),
    "cheek": (255, 128, 150, 255),
    "sting": (120, 112, 130, 255),
}

# layer map: z order, larger z draws on top
grid = [[None] * N for _ in range(N)]   # (shape, z)


def put(x, y, shape, z):
    if 0 <= x < N and 0 <= y < N:
        cur = grid[y][x]
        if cur is None or cur[1] <= z:
            grid[y][x] = (shape, z)


def ellipse(cx, cy, rx, ry, shape, z, clip=None):
    for y in range(N):
        for x in range(N):
            dx = (x + 0.5 - cx) / rx
            dy = (y + 0.5 - cy) / ry
            if dx * dx + dy * dy <= 1.0 and (clip is None or clip(x, y)):
                put(x, y, shape, z)


# wings (behind body)
ellipse(10.5, 9.5, 4.6, 6.2, "wing_back", 1)
ellipse(15.5, 10.0, 4.2, 5.6, "wing_front", 2)
# stinger
for x, y in [(2, 20), (3, 20), (3, 19), (3, 21), (4, 20)]:
    put(x, y, "sting", 3)
# abdomen + thorax
ellipse(13.0, 20.5, 9.6, 7.2, "body", 4)
# legs
for x, y in [(9, 28), (10, 28), (14, 28), (15, 28), (19, 27), (20, 28)]:
    put(x, y, "leg", 3)
# head
ellipse(22.5, 17.2, 6.4, 6.0, "head", 5)
# hard hat dome + brim
ellipse(22.5, 12.8, 6.8, 6.0, "hat", 6, clip=lambda x, y: y <= 12)
for x in range(15, 30):
    put(x, 13, "brim", 7)

canvas = [[None] * N for _ in range(N)]


def shade(shape, x, y):
    if shape == "body":
        # stripes
        if 7 <= x <= 9 or 13 <= x <= 15:
            base, hi, lo = "black", "black_hi", "black"
        else:
            base, hi, lo = "yellow", "yellow_hi", "yellow_lo"
        if y <= 15:
            return hi
        if y >= 25:
            return lo
        return base
    if shape == "head":
        if y >= 21 or x >= 27:
            return "yellow_lo"
        if y <= 14 and x <= 20:
            return "yellow_hi"
        return "yellow"
    if shape in ("wing_back", "wing_front"):
        if y <= 6 and shape == "wing_front":
            return "wing_hi"
        if y >= 13:
            return "wing_lo"
        return "wing"
    if shape == "hat":
        if x <= 20 and y <= 10:
            return "hat_hi"
        if x >= 26:
            return "hat_lo"
        return "hat"
    if shape == "brim":
        return "hat_lo"
    if shape in ("leg", "antenna"):
        return "outline"
    if shape == "sting":
        return "sting"
    return "outline"


for y in range(N):
    for x in range(N):
        g = grid[y][x]
        if g:
            canvas[y][x] = PAL[shade(g[0], x, y)]

# internal borders: a pixel whose 4-neighbour belongs to a higher layer of a different shape
for y in range(N):
    for x in range(N):
        g = grid[y][x]
        if not g or g[0] in ("leg", "antenna", "brim"):
            continue
        for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
            nx, ny = x + dx, y + dy
            if 0 <= nx < N and 0 <= ny < N:
                n = grid[ny][nx]
                if n and n[0] != g[0] and n[1] > g[1] and n[0] not in ("leg", "antenna"):
                    canvas[y][x] = PAL["outline"]
                    break

# outer outline
filled = [[canvas[y][x] is not None for x in range(N)] for y in range(N)]
for y in range(N):
    for x in range(N):
        if filled[y][x]:
            continue
        for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
            nx, ny = x + dx, y + dy
            if 0 <= nx < N and 0 <= ny < N and filled[ny][nx]:
                canvas[y][x] = PAL["outline"]
                break

# antennae: 1px stalks from the hat, 2x2 knobs
for x, y in [(19, 5), (18, 4), (17, 3), (25, 5), (26, 4), (27, 3)]:
    canvas[y][x] = PAL["outline"]
for x, y in [(15, 1), (16, 1), (15, 2), (16, 2), (28, 1), (29, 1), (28, 2), (29, 2)]:
    canvas[y][x] = PAL["outline"]
canvas[1][15] = PAL["black_hi"]
canvas[1][28] = PAL["black_hi"]

# face: eye, highlight, smile, cheek
for x, y in [(24, 15), (25, 15), (24, 16), (25, 16), (24, 17), (25, 17)]:
    canvas[y][x] = PAL["outline"]
canvas[15][24] = PAL["white"]
for x, y in [(23, 20), (24, 21), (25, 21), (26, 20)]:
    canvas[y][x] = PAL["outline"]
canvas[19][27] = PAL["cheek"]
canvas[19][21] = PAL["cheek"]
# hat ridge
for y in range(8, 13):
    canvas[y][22] = PAL["hat_lo"] if y > 8 else PAL["hat_hi"]

img = Image.new("RGBA", (N, N), (0, 0, 0, 0))
for y in range(N):
    for x in range(N):
        if canvas[y][x]:
            img.putpixel((x, y), canvas[y][x])

OUT.mkdir(parents=True, exist_ok=True)
img.save(OUT / "werkrby-32.png")
for s in (64, 128, 256, 512, 1024):
    img.resize((s, s), Image.NEAREST).save(OUT / f"werkrby-{s}.png")
img.resize((256, 256), Image.NEAREST).save(
    OUT / "werkrby.ico", sizes=[(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)])

# social preview 1280x640: icon left, pixel wordmark right
prev = Image.new("RGBA", (1280, 640), (255, 244, 214, 255))
big = img.resize((448, 448), Image.NEAREST)
prev.alpha_composite(big, (80, 96))
FONT = {
    "W": ["10001", "10001", "10001", "10101", "10101", "11011", "10001"],
    "E": ["11111", "10000", "10000", "11110", "10000", "10000", "11111"],
    "R": ["11110", "10001", "10001", "11110", "10100", "10010", "10001"],
    "K": ["10001", "10010", "10100", "11000", "10100", "10010", "10001"],
    "B": ["11110", "10001", "10001", "11110", "10001", "10001", "11110"],
    "Y": ["10001", "10001", "01010", "00100", "00100", "00100", "00100"],
    "O": ["01110", "10001", "10001", "10001", "10001", "10001", "01110"],
    " ": ["00000"] * 7,
}


def text(word, x0, y0, px, color):
    for i, ch in enumerate(word):
        for r, row in enumerate(FONT[ch]):
            for c, bit in enumerate(row):
                if bit == "1":
                    for yy in range(px):
                        for xx in range(px):
                            prev.putpixel((x0 + i * 6 * px + c * px + xx, y0 + r * px + yy), color)


text("WERKRBY", 580, 236, 14, PAL["outline"])
text("WORKER BEE", 584, 366, 6, PAL["hat_lo"])
prev.save(OUT / "werkrby-social.png")
print("ok", OUT)
