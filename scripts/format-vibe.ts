import { readdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { format } from 'oxfmt';
let changed = false;
async function visit(directory: string) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) await visit(path);
    else if (path.endsWith('.vibe')) {
      const original = await readFile(path, 'utf8');
      // Use the formatter's compatible SFC parser, without importing a second UI runtime.
      const result = await format(`${path}.vue`, original, { singleQuote: true });
      if (result.errors.length) throw new Error(`Cannot format ${path}`);
      if (result.code !== original) {
        if (process.argv.includes('--write')) await writeFile(path, result.code);
        else {
          changed = true;
          console.error(`Unformatted: ${path}`);
        }
      }
    }
  }
}
await visit('site');
if (changed) process.exitCode = 1;
