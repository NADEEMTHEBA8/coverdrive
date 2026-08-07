import resvg_py

print("Rendering 8K Super-HD Zero-Pixelation PNG with resvg-py (zoom=8.0)...")
with open(
    "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v3.svg",
    encoding="utf-8",
) as f:
    svg_str = f.read()

# Render at 8.0x DPI scale (8K Retina Output - Zero Pixelation when Zooming!)
png_bytes = resvg_py.svg_to_bytes(svg_str, zoom=8.0)

output_path = "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_aws_cloud_v3.png"
alt_output_path = (
    "/Users/nadeemtheba/projects/coverdrive/docs/assets/coverdrive_architecture_diagram.png"
)

with open(output_path, "wb") as f:
    f.write(png_bytes)

with open(alt_output_path, "wb") as f:
    f.write(png_bytes)

print(f"8K Super-HD Crisp PNG saved successfully ({len(png_bytes) / 1024 / 1024:.2f} MB)!")
