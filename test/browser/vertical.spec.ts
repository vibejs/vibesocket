import { expect, test } from '@playwright/test';
test('two independent .vibe clients exchange a real validated WebSocket event', async ({ browser }) => {
  const a = await browser.newContext(), b = await browser.newContext();
  const first = await a.newPage(), second = await b.newPage();
  await Promise.all([first.goto('/'), second.goto('/')]);
  await expect(first.getByTestId('status')).toHaveText('connected');
  await expect(second.getByTestId('status')).toHaveText('connected');
  await first.getByLabel('Message', { exact: true }).fill('Two browsers, one room');
  await first.getByRole('button', { name: 'Send message' }).click();
  await expect(second.getByText('Two browsers, one room', { exact: true })).toBeVisible();
  await a.close(); await b.close();
});
