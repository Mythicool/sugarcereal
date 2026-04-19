import { mkdir, rm, copyFile } from "node:fs/promises";
import { resolve } from "node:path";

const root = process.cwd();
const distDir = resolve(root, "dist");
const webDir = resolve(root, "web");
const imagesDir = resolve(root, "images");
const distImagesDir = resolve(distDir, "images");

await rm(distDir, { recursive: true, force: true });
await mkdir(distDir, { recursive: true });
await mkdir(distImagesDir, { recursive: true });

await copyFile(resolve(webDir, "index.html"), resolve(distDir, "index.html"));
await copyFile(resolve(webDir, "styles.css"), resolve(distDir, "styles.css"));
await copyFile(resolve(webDir, "app.js"), resolve(distDir, "app.js"));
await copyFile(resolve(root, "fancy_serial_analyzer.py"), resolve(distDir, "fancy_serial_analyzer.py"));
await copyFile(resolve(root, "birthdays.csv"), resolve(distDir, "birthdays.csv"));
await copyFile(resolve(root, "World Important Dates.csv"), resolve(distDir, "World Important Dates.csv"));
await copyFile(resolve(root, "disorder_events_sample.csv"), resolve(distDir, "disorder_events_sample.csv"));
await copyFile(resolve(root, "us_public_holidays.csv"), resolve(distDir, "us_public_holidays.csv"));
await copyFile(resolve(root, "us_zip_reference.csv"), resolve(distDir, "us_zip_reference.csv"));
await copyFile(resolve(imagesDir, "banner.png"), resolve(distImagesDir, "banner.png"));

console.log("Built web app assets in dist/");
