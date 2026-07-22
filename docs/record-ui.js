// Records the Freshet UI answering a question over REAL status-feed incidents.
// Assumes the live-demo stack is already serving on :8000 with data indexed.
const { chromium } = require('playwright');

const OUT = process.env.OUT_DIR || '.';
const QUESTION = "what is wrong with Cloudflare Access?";

(async () => {
  const browser = await chromium.launch();
  const ctx = await browser.newContext({
    viewport: { width: 1280, height: 1020 },
    deviceScaleFactor: 2,
    recordVideo: { dir: OUT, size: { width: 1280, height: 1020 } },
  });
  const page = await ctx.newPage();

  await page.goto('http://localhost:8000', { waitUntil: 'networkidle' });

  // let the live feed populate with real incidents + the gauges tick
  await page.waitForSelector('#feed .card, #feed article, #feed > *:not(.empty)', { timeout: 30000 })
    .catch(() => console.log('note: feed selector not matched, continuing'));
  await page.waitForTimeout(4000);

  // type the question at a human pace
  await page.click('#question');
  await page.type('#question', QUESTION, { delay: 55 });
  await page.waitForTimeout(700);

  // submit and wait for the grounded answer to render
  await page.click('#ask');
  await page.waitForFunction(
    () => {
      const a = document.querySelector('#answer');
      return a && !a.querySelector('.idle') && a.innerText.trim().length > 80;
    },
    { timeout: 90000 }
  ).catch(() => console.log('note: answer wait timed out'));

  // no scrolling: the taller viewport fits header + question + answer +
  // sources, and static frames keep the GIF small
  await page.waitForTimeout(7000);

  await ctx.close();
  await browser.close();
  console.log('done');
})();
