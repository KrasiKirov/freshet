// Records the Freshet UI answering a question over REAL status-feed incidents.
// Assumes the live-demo stack is already serving on :8000 with data indexed.
//
//   node docs/record-ui.js            # writes a .webm next to this script
//
// Then encode to the committed GIF. 880px matches the width GitHub renders a
// README image at, and the UI is dark and near-greyscale so 16 colours quantise
// with no visible banding (this is ~57% smaller than a 1000px/32-colour encode):
//
//   ffmpeg -i page.webm -vf "fps=5,scale=880:-1:flags=lanczos,\
//     palettegen=max_colors=16:stats_mode=diff" -y pal.png
//   ffmpeg -i page.webm -i pal.png -lavfi "fps=5,scale=880:-1:flags=lanczos,\
//     paletteuse=dither=none" -y docs/live-demo.gif
const { chromium } = require('playwright');

const OUT = process.env.OUT_DIR || '.';
const QUESTION = "what is happening with the Network Performance Issues in Istanbul?";

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
  // sources, and static frames keep the GIF small. ~3s is enough to read the
  // final state; a longer hold only adds frames.
  await page.waitForTimeout(3500);

  await ctx.close();
  await browser.close();
  console.log('done');
})();
