import fs from 'node:fs';
import type {AstroIntegration} from 'astro';
import * as prettier from 'prettier';

/**
 * Options for the notebook plugin.
 * @param input The Jupyter notebook. Its first line must be the title as a `# ` heading.
 * @param output The markdown page to write.
 * @param sidebarOrder The position of the page in its sidebar group.
 */
interface NotebookOptions {
    input: string;
    output: string;
    sidebarOrder: number;
}

interface Cell {
    cell_type: 'markdown' | 'code' | 'raw';
    source: string | string[];
}

/**
 * Astro integration that turns a Jupyter notebook into a docs page, so the code on the page is the code that CI runs.
 * Markdown cells are copied, code cells become Python code blocks, outputs are dropped.
 * @param options Options for the notebook plugin.
 * @returns An Astro integration that fires on hook `config:setup`
 */
export default function notebookPlugin(options: NotebookOptions): AstroIntegration {
    return {
        name: 'notebook-plugin',
        hooks: {
            'astro:config:setup': async ({isRestart, logger}) => {
                if (isRestart) return;

                const {input, output, sidebarOrder} = options;

                logger.info(`Generating ${output} from ${input}...`);

                const cells: Cell[] = JSON.parse(fs.readFileSync(input, 'utf-8')).cells;
                const text = (cell: Cell): string => (Array.isArray(cell.source) ? cell.source.join('') : cell.source).trim();

                const [heading, ...body] = cells
                    .filter((cell) => cell.cell_type !== 'raw')
                    .map((cell) => (cell.cell_type === 'code' ? `\`\`\`python\n${text(cell)}\n\`\`\`` : text(cell)))
                    .join('\n\n')
                    .split('\n');

                if (!heading.startsWith('# ')) {
                    throw new Error(`${input} must start with the title as a "# " heading`);
                }

                const page = `---
title: ${heading.slice(2)}
sidebar:
    order: ${sidebarOrder}
---

<!-- Generated from ${input} by plugins/notebook.ts. Do not edit. -->

${body.join('\n').trim()}
`;

                const config = await prettier.resolveConfig(output);
                fs.writeFileSync(output, await prettier.format(page, {...config, parser: 'markdown', filepath: output}));
            },
        },
    };
}
