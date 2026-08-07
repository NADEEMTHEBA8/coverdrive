const puppeteer = require('puppeteer');
const path = require('path');
const fs = require('fs');

(async () => {
    console.log('Starting Ultra HD 4K/8K PNG Renderer...');
    const browser = await puppeteer.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox']
    });
    const page = await browser.newPage();

    // Set 4K Retina Viewport (3840 x 2160 with 3x Device Scale Factor = 11,520 x 6,480 resolution!)
    await page.setViewport({
        width: 3840,
        height: 2160,
        deviceScaleFactor: 3
    });

    const svgPath = path.join(__dirname, 'assets', 'coverdrive_aws_cloud_v3.svg');
    const svgContent = fs.readFileSync(svgPath, 'utf8');

    // Load SVG in HTML with white background
    const htmlContent = `
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {
                    margin: 0;
                    padding: 40px;
                    background-color: #FAFAFA;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    min-height: 100vh;
                    font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
                }
                svg {
                    width: 100%;
                    height: auto;
                    max-width: 3600px;
                }
            </style>
        </head>
        <body>
            ${svgContent}
        </body>
        </html>
    `;

    await page.setContent(htmlContent, { waitUntil: 'networkidle0' });

    // Target the SVG container bounding box
    const element = await page.$('svg');
    const boundingBox = await element.boundingBox();

    const outputPath = path.join(__dirname, 'assets', 'coverdrive_aws_cloud_v3.png');
    const altOutputPath = path.join(__dirname, 'assets', 'coverdrive_architecture_diagram.png');

    await element.screenshot({
        path: outputPath,
        omitBackground: false,
        type: 'png'
    });

    fs.copyFileSync(outputPath, altOutputPath);

    console.log('Ultra HD 4K PNG saved successfully at:', outputPath);
    await browser.close();
})();
