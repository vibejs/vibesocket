const sources = import.meta.glob('../docs/**/*.md', {
  eager: true,
  query: '?raw',
  import: 'default',
}) as Record<string, string>;
export const documents = Object.entries(sources).map(([file, content]) => ({
  id: file.split('/').at(-1)!.replace('.md', ''),
  title: content.match(/^# (.+)/)?.[1] ?? file,
  content,
}));
export function blocks(content: string) {
  const result: {
    kind: 'heading' | 'text' | 'code' | 'table' | 'list';
    text: string;
    level: number;
    rows: string[][];
    items: string[];
    ordered: boolean;
  }[] = [];
  const block = (kind: (typeof result)[number]['kind'], text = '', level = 0) => ({
    kind,
    text,
    level,
    rows: [] as string[][],
    items: [] as string[],
    ordered: false,
  });
  let code = false,
    buffer: string[] = [];
  for (const line of content.split('\n')) {
    if (line.startsWith('```')) {
      if (code) {
        result.push(block('code', buffer.join('\n')));
        buffer = [];
      }
      code = !code;
      continue;
    }
    if (code) {
      buffer.push(line);
      continue;
    }
    if (!line.trim()) continue;
    if (line.startsWith('|')) {
      const cells = line
        .trim()
        .replace(/^\||\|$/g, '')
        .split('|')
        .map((cell) => cell.trim());
      if (cells.every((cell) => /^:?-+:?$/.test(cell))) continue;
      if (result.at(-1)?.kind !== 'table') result.push(block('table'));
      result.at(-1)!.rows.push(cells);
      continue;
    }
    const item = line.match(/^(- |[0-9]+\. )(.*)$/);
    if (item) {
      const ordered = item[1] !== '- ';
      if (result.at(-1)?.kind !== 'list' || result.at(-1)?.ordered !== ordered) {
        const list = block('list');
        list.ordered = ordered;
        result.push(list);
      }
      result.at(-1)!.items.push(item[2]);
      continue;
    }
    if (line.startsWith('#'))
      result.push(block('heading', line.replace(/^#+ /, ''), line.match(/^#+/)![0].length));
    else result.push(block('text', line));
  }
  return result;
}
export function inlineTokens(text: string) {
  const tokens: { kind: 'text' | 'code' | 'bold' | 'link'; text: string; href: string }[] = [];
  let end = 0;
  for (const match of text.matchAll(/(`[^`]+`|\*\*[^*]+\*\*|\[[^\]]+\]\(https?:\/\/[^)]+\))/g)) {
    if (match.index > end)
      tokens.push({ kind: 'text', text: text.slice(end, match.index), href: '' });
    const value = match[0],
      link = value.match(/^\[([^\]]+)\]\((.+)\)$/);
    tokens.push(
      link
        ? { kind: 'link', text: link[1], href: link[2] }
        : {
            kind: value.startsWith('`') ? 'code' : 'bold',
            text: value.startsWith('`') ? value.slice(1, -1) : value.slice(2, -2),
            href: '',
          },
    );
    end = match.index + value.length;
  }
  if (end < text.length) tokens.push({ kind: 'text', text: text.slice(end), href: '' });
  return tokens;
}
