import { expect, test, type Page } from '@playwright/test';
async function login(page: Page, name = 'Ada') {
  await page.goto('/');
  const connect = page.getByRole('button', { name: `Connect as ${name}` });
  if (await connect.isVisible()) await connect.click();
  await expect(page.getByTestId('status')).toHaveText('connected');
}
test('two independent .vibe clients exchange and persist acknowledged messages', async ({
  browser,
}) => {
  const a = await browser.newContext(),
    b = await browser.newContext();
  const first = await a.newPage(),
    second = await b.newPage();
  await Promise.all([login(first), login(second, 'Lin')]);
  const message = `Two browsers, one room ${crypto.randomUUID()}`;
  await first.getByLabel('Message', { exact: true }).fill(message);
  await first.getByRole('button', { name: 'Send message' }).click();
  await expect(second.getByText(message, { exact: true })).toBeVisible();
  await second.reload();
  await expect(second.getByText(message, { exact: true })).toBeVisible();
  await a.close();
  await b.close();
});
test('reconnect, room changes, page navigation and logout release connection state', async ({
  page,
}) => {
  await login(page);
  await page.getByRole('button', { name: 'Disconnect', exact: true }).click();
  await expect(page.getByTestId('status')).toHaveText('disconnected');
  await page.getByRole('button', { name: 'Reconnect', exact: false }).click();
  await expect(page.getByTestId('status')).toHaveText('connected');
  await page.evaluate(() =>
    (
      window as unknown as { demo: { socket: { rawSocket: WebSocket } } }
    ).demo.socket.rawSocket.close(4000, 'test loss'),
  );
  await expect(page.getByTestId('status')).toHaveText('connected');
  await page.getByRole('button', { name: '# Engineering' }).click();
  await expect(page.getByRole('heading', { name: '# Engineering' })).toBeVisible();
  await page.getByRole('button', { name: 'Sign out' }).click();
  await expect(page.getByRole('button', { name: 'Connect as Ada' })).toBeVisible();
  await expect(page.getByTestId('status')).toHaveText('disconnected');
});
test('Kanban reconciles authoritative updates in a second browser', async ({ browser }) => {
  const a = await browser.newContext(),
    b = await browser.newContext();
  const first = await a.newPage(),
    second = await b.newPage();
  await Promise.all([login(first), login(second, 'Lin')]);
  await first.getByRole('button', { name: '▤ Kanban' }).click();
  await second.getByRole('button', { name: '▤ Kanban' }).click();
  const title = `Shared task ${crypto.randomUUID().slice(0, 8)}`;
  await first.getByLabel('New task').fill(title);
  await first.getByRole('button', { name: 'Create task' }).click();
  await expect(second.getByRole('heading', { name: title })).toBeVisible();
  const task = first.locator('.task').filter({ hasText: title });
  await task.getByRole('button', { name: 'Move task forward' }).click();
  await expect(
    second
      .locator('.column')
      .filter({ has: second.getByRole('heading', { name: 'In progress', exact: false }) })
      .getByRole('heading', { name: title }),
  ).toBeVisible();
  await a.close();
  await b.close();
});
test('SSE metrics, pause/resume, named token events and cancellation work', async ({ page }) => {
  await login(page);
  await page.getByRole('button', { name: '⌁ Dashboard' }).click();
  await expect(page.locator('.metric-row').first()).toBeVisible();
  await page.getByRole('button', { name: 'Pause updates' }).click();
  await expect(page.getByText(/Updates paused/)).toBeVisible();
  await page.getByRole('button', { name: 'Resume updates' }).click();
  await page.getByRole('button', { name: '≋ Streaming' }).click();
  await page.getByRole('button', { name: 'Start stream' }).click();
  await expect(page.locator('.stream-output')).toContainText('A small protocol.');
  await page.getByRole('button', { name: 'Cancel stream' }).click();
  await expect(page.getByText('Cancelled', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Start stream' }).click();
  await expect(page.getByText('Complete', { exact: true })).toBeVisible();
});
test('organization isolation is enforced on subscriptions, history and role permissions', async ({
  browser,
}) => {
  const context = await browser.newContext(),
    page = await context.newPage();
  await login(page, 'Eve');
  const code = await page.evaluate(async () => {
    const socket = (
      window as unknown as {
        demo: { socket: { request(event: string, input: unknown): Promise<unknown> } };
      }
    ).demo.socket;
    try {
      await socket.request('history', { room: 'chat:orbit:general' });
      return 'allowed';
    } catch (error) {
      return (error as { code: string }).code;
    }
  });
  expect(code).toBe('FORBIDDEN');
  await context.close();
});
test('searchable docs, theme and localization are interactive', async ({ page }) => {
  await login(page);
  await page.getByRole('button', { name: 'EN', exact: true }).click();
  await expect(page.getByRole('heading', { name: '# Genel' })).toBeVisible();
  await page.getByRole('button', { name: 'Toggle color theme' }).click();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
  await page.getByLabel('Search documentation').fill('upgrade');
  await expect(page.locator('.doc-nav button').first()).toBeVisible();
});

test('multiple Vibe apps isolate plugins and release scopes, page lifecycle and offline retry', async ({
  page,
  context,
}) => {
  await login(page);
  await page.evaluate(async () => {
    const path = '/isolation.ts';
    const { mountIsolated } = await import(path);
    const a = document.createElement('div'),
      b = document.createElement('div');
    a.id = 'isolation-a';
    b.id = 'isolation-b';
    document.body.append(a, b);
    Object.assign(window, { isolatedApps: [mountIsolated(a), mountIsolated(b)] });
  });
  await expect(page.locator('#isolation-a')).toHaveText('connected');
  await expect(page.locator('#isolation-b')).toHaveText('connected');
  await page.evaluate(() => {
    (
      window as unknown as { isolatedApps: { app: { unmount(): void } }[] }
    ).isolatedApps[0].app.unmount();
  });
  await expect(page.locator('#isolation-a')).toBeEmpty();
  await expect(page.locator('#isolation-b')).toHaveText('connected');
  await expect(page.getByTestId('status')).toHaveText('connected');
  await page.evaluate(() => {
    window.dispatchEvent(new PageTransitionEvent('pagehide', { persisted: true }));
  });
  await expect(page.locator('#isolation-b')).toHaveText('disconnected');
  await page.evaluate(() => {
    window.dispatchEvent(new PageTransitionEvent('pageshow', { persisted: true }));
  });
  await expect(page.locator('#isolation-b')).toHaveText('connected');
  await context.setOffline(true);
  await page.evaluate(() =>
    (
      window as unknown as { demo: { socket: { rawSocket: WebSocket } } }
    ).demo.socket.rawSocket.close(4000, 'offline fixture'),
  );
  await expect(page.getByTestId('status')).toHaveText('reconnecting');
  await context.setOffline(false);
  await expect(page.getByTestId('status')).toHaveText('connected');
  await page.evaluate(() => {
    (
      window as unknown as { isolatedApps: { app: { unmount(): void } }[] }
    ).isolatedApps[1].app.unmount();
  });
});

test('responsive workbench stays within a narrow viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await login(page);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(
    true,
  );
  await page.getByLabel('Message', { exact: true }).fill('');
  await expect(page.getByRole('button', { name: 'Send message' })).toBeDisabled();
});

test('pointer drag commits a single authoritative Kanban move', async ({ page }) => {
  await login(page);
  await page.getByRole('button', { name: '▤ Kanban' }).click();
  const title = `Drag task ${crypto.randomUUID().slice(0, 8)}`;
  await page.getByLabel('New task').fill(title);
  await page.getByRole('button', { name: 'Create task' }).click();
  const source = page.locator('.task').filter({ hasText: title });
  await expect(source).toBeVisible();
  await source.scrollIntoViewIfNeeded();
  const target = page
    .locator('.column')
    .filter({ has: page.getByRole('heading', { name: 'In progress', exact: false }) });
  const from = await source.boundingBox(),
    to = await target.boundingBox();
  if (!from || !to) throw new Error('Missing drag geometry');
  await page.mouse.move(from.x + from.width / 2, from.y + 35);
  await page.mouse.down();
  await expect(source).toHaveAttribute('data-dragging', 'true');
  await page.mouse.move(from.x + from.width / 2 + 20, from.y + 40, { steps: 5 });
  await page.mouse.move(
    to.x + to.width / 2,
    Math.max(to.y + 45, Math.min(to.y + to.height - 20, from.y + 40)),
    { steps: 15 },
  );
  await expect
    .poll(() =>
      target.evaluate(
        (element) =>
          element.matches('[data-over="true"]') || !!element.querySelector('[data-over="true"]'),
      ),
    )
    .toBe(true);
  await page.mouse.up();
  await expect(target.getByRole('heading', { name: title })).toBeVisible();
});
