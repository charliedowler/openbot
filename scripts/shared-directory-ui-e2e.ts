import assert from "node:assert/strict";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { ChannelClient, connectChannelE2e } from "./channel-e2e-support";

const session = await connectChannelE2e("shared-directory-ui-e2e");
const { page } = session;
page.setDefaultTimeout(10_000);
const client = new ChannelClient(page);
const suffix = crypto.randomUUID().slice(0, 8);
const folderName = `shared-browser-${suffix}`;
const fixture = join(homedir(), "OpenBot", "Shared", folderName);
const output = resolve(".openbot-build/shared-directory-ui-e2e");
const svg =
  '<svg xmlns="http://www.w3.org/2000/svg" width="640" height="400" viewBox="0 0 640 400"><rect width="640" height="400" fill="#f8fafc"/><text x="48" y="60" font-family="sans-serif" font-size="26" fill="#172033">Shared evidence preview</text><rect x="70" y="180" width="100" height="160" fill="#6366f1"/><rect x="270" y="120" width="100" height="220" fill="#10b981"/><rect x="470" y="90" width="100" height="250" fill="#38bdf8"/><text x="48" y="380" font-family="sans-serif" font-size="16" fill="#475569">Synthetic fixture. Opened directly in OpenBot.</text></svg>';
await mkdir(join(fixture, "nested"), { recursive: true });
await mkdir(output, { recursive: true });
await writeFile(join(fixture, "dashboard.svg"), svg);
await writeFile(
  join(fixture, "landscape.svg"),
  '<svg xmlns="http://www.w3.org/2000/svg" width="2200" height="600"><rect width="2200" height="600" fill="#e0f2fe"/><text x="40" y="100" font-size="60">LEFT EDGE</text><text x="1740" y="540" font-size="60">RIGHT EDGE</text></svg>',
);
await writeFile(
  join(fixture, "portrait.svg"),
  '<svg xmlns="http://www.w3.org/2000/svg" width="600" height="2200"><rect width="600" height="2200" fill="#dcfce7"/><text x="40" y="100" font-size="50">TOP EDGE</text><text x="40" y="2140" font-size="50">BOTTOM EDGE</text></svg>',
);
await writeFile(join(fixture, "README.md"), "# Shared evidence\n\nThis file opens inside OpenBot.\n");
await writeFile(join(fixture, "nested", "notes.txt"), "Nested Shared folder navigation.\n");
const errors: string[] = [];
page.on("pageerror", (error) => errors.push(error.message));
const panel = page.getByRole("complementary", { name: "File preview", exact: true });
async function dismissNotifications() {
  const close = page.getByRole("button", { name: "Close notification", exact: true });
  for (let count = 0; count < 10 && (await close.count()) > 0; count += 1) {
    const button = close.first();
    await button.press("Enter");
    await button.waitFor({ state: "detached" });
  }
}
try {
  await dismissNotifications();
  const channel = await client.create(`Shared files ${suffix}`, [], null);
  await client.send(channel.id, `Evidence files: ${fixture}/`, null);
  await page
    .getByRole("navigation", { name: "Chat list" })
    .getByRole("button", { name: new RegExp(`^Shared files ${suffix}`) })
    .click();
  await page
    .getByRole("main", { name: "Conversation" })
    .getByRole("heading", { level: 1, name: `Shared files ${suffix}`, exact: true })
    .waitFor();
  const link = page
    .getByRole("main", { name: "Conversation" })
    .getByRole("button", { name: /^Open shared file / })
    .first();
  await link.click();
  await panel.getByRole("heading", { name: folderName, exact: true }).waitFor();
  const files = panel.getByRole("list", { name: folderName, exact: true });
  await files.getByRole("button", { name: /dashboard.svg/ }).waitFor();
  await dismissNotifications();
  await panel.screenshot({ path: join(output, "folder.png") });
  await files.getByRole("button", { name: /dashboard.svg/ }).click();
  await panel.getByRole("heading", { name: "dashboard.svg", exact: true }).waitFor();
  const image = panel.getByRole("img", { name: "dashboard.svg", exact: true });
  await image.waitFor();
  await image.evaluate(
    (element) =>
      new Promise<void>((done, fail) => {
        if (!(element instanceof HTMLImageElement)) {
          fail(new Error("Image renderer is missing"));
          return;
        }
        if (element.complete && element.naturalWidth > 0) {
          done();
          return;
        }
        element.addEventListener("load", () => done(), { once: true });
        element.addEventListener("error", () => fail(new Error("Image bytes did not render")), { once: true });
      }),
  );
  await dismissNotifications();
  await panel.screenshot({ path: join(output, "image.png") });
  await page.keyboard.press("ArrowRight");
  await panel.getByRole("heading", { name: "landscape.svg", exact: true }).waitFor();
  await panel.getByRole("img", { name: "landscape.svg", exact: true }).evaluate(async (element) => {
    if (!(element instanceof HTMLImageElement)) throw new Error("Image renderer is missing");
    await element.decode();
  });
  await panel.screenshot({ path: join(output, "image-fit-landscape.png") });
  await page.keyboard.press("ArrowRight");
  await panel.getByRole("heading", { name: "portrait.svg", exact: true }).waitFor();
  await panel.getByRole("img", { name: "portrait.svg", exact: true }).evaluate(async (element) => {
    if (!(element instanceof HTMLImageElement)) throw new Error("Image renderer is missing");
    await element.decode();
  });
  await panel.screenshot({ path: join(output, "image-fit-portrait.png") });
  await page.keyboard.press("ArrowRight");
  await panel.getByRole("heading", { name: "README.md", exact: true }).waitFor();
  await page.keyboard.press("ArrowRight");
  await panel.getByRole("heading", { name: "dashboard.svg", exact: true }).waitFor();
  await page.keyboard.press("ArrowLeft");
  await panel.getByRole("heading", { name: "README.md", exact: true }).waitFor();
  const composer = page.getByRole("textbox", { name: "Message to channel", exact: true });
  await composer.fill("Navigation stays in this draft");
  await composer.press("ArrowLeft");
  assert.equal(await panel.getByRole("heading", { level: 2 }).innerText(), "README.md");
  await composer.fill("");
  await panel.getByRole("button", { name: "Next file", exact: true }).click();
  await panel.getByRole("heading", { name: "dashboard.svg", exact: true }).waitFor();
  await page.keyboard.press("ArrowRight");
  await page.keyboard.press("ArrowRight");
  await panel.getByRole("heading", { name: "portrait.svg", exact: true }).waitFor();

  await panel.getByRole("button", { name: "Back", exact: true }).click();
  await panel.getByRole("heading", { name: folderName, exact: true }).waitFor();
  await panel
    .getByRole("list")
    .getByRole("button", { name: /README.md/ })
    .click();
  await panel.getByRole("heading", { name: "README.md", exact: true }).waitFor();
  await panel.getByRole("heading", { name: "Shared evidence", exact: true }).waitFor();
  await panel.getByRole("button", { name: "Back", exact: true }).click();
  await panel.getByRole("heading", { name: folderName, exact: true }).waitFor();
  await dismissNotifications();
  await panel
    .getByRole("list")
    .getByRole("button", { name: /nested/ })
    .click();
  await panel.getByRole("heading", { name: "nested", exact: true }).waitFor();
  await panel
    .getByRole("list")
    .getByRole("button", { name: /notes.txt/ })
    .click();
  await panel.getByRole("heading", { name: "notes.txt", exact: true }).waitFor();
  assert.ok((await panel.innerText()).includes("Nested Shared folder navigation."));
  await panel.getByRole("button", { name: "Back", exact: true }).click();
  await panel.getByRole("heading", { name: "nested", exact: true }).waitFor();
  await panel.getByRole("button", { name: "Back", exact: true }).click();
  await panel.getByRole("heading", { name: folderName, exact: true }).waitFor();
  await panel.getByRole("button", { name: "Close file preview", exact: true }).click();
  await panel.waitFor({ state: "detached" });
  await link.click();
  await panel.getByRole("heading", { name: folderName, exact: true }).waitFor();
  assert.deepEqual(errors, []);
  await writeFile(
    join(output, "report.json"),
    JSON.stringify(
      {
        passed: true,
        checks: [
          "real Shared folder link",
          "native folder list",
          "rendered image bytes",
          "Left and Right file navigation",
          "forward and reverse wraparound excluding folders",
          "rapid navigation keeps the latest request",
          "chat typing does not navigate the preview",
          "Next file toolbar action",
          "Markdown preview",
          "nested file preview",
          "Back to containing folder",
          "parent navigation",
          "close and reopen",
        ],
        screenshots: ["folder.png", "image.png", "image-fit-landscape.png", "image-fit-portrait.png"],
      },
      null,
      2,
    ),
  );
  process.stdout.write(`Shared directory UI E2E passed: ${output}\n`);
} catch (error) {
  await page.screenshot({ path: join(output, "failure.png") });
  await writeFile(
    join(output, "report.json"),
    JSON.stringify({ passed: false, error: String(error), pageErrors: errors }, null, 2),
  );
  throw error;
} finally {
  await client.cleanup();
  await session.close();
  await rm(fixture, { recursive: true, force: true });
}
