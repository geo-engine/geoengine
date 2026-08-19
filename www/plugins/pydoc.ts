import fs from 'node:fs';
import path from 'node:path';
import {execSync} from 'node:child_process';
import type {AstroIntegration} from 'astro';
import * as prettier from 'prettier';

/**
 * Options for the Pydoc plugin.
 * @param outputDir The directory where the generated markdown files will be saved.
 */
interface PydocOptions {
    outputDir: string;
}

/**
 * Astro integration that generates markdown documentation for a Python package.
 * It reads the Python package, extracts operator definitions, and writes markdown files for each operator
 * in the specified output directory.
 * @param options Options for the Pydoc plugin.
 * @returns An Astro integration that fires on hook `config:setup`
 */
export default function pydocPlugin(options: PydocOptions): AstroIntegration {
    return {
        name: 'pydoc-plugin',
        hooks: {
            'astro:config:setup': async ({command: _, isRestart, logger}) => {
                // Prevent double-running during simple dev reloads
                if (isRestart) return;

                const {outputDir} = options;

                logger.info(`Generating pydoc...`);

                try {
                    clearDirButKeepIndex(outputDir);

                    execSync('pipx run pydoc-markdown pydoc-markdown.yml');

                    fs.rmSync(path.join(outputDir, 'sidebar.json'));

                    await formatDirWithPrettier(outputDir);

                    logger.info('✅ Documentation generated successfully.');
                } catch (error) {
                    logger.error(`Failed to generate docs: ${error}`);
                }
            },
        },
    };
}

/**
 * Clears the contents of the output folders but keeps existing index.md files.
 * @param dirPath The path to the directory to be cleared.
 */
function clearDirButKeepIndex(dirPath: string): void {
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, {recursive: true});
        return;
    }
    for (const entry of fs.readdirSync(dirPath)) {
        if (entry === 'index.md') continue;
        const full = path.join(dirPath, entry);
        fs.rmSync(full, {recursive: true, force: true});
    }
}

async function formatDirWithPrettier(dirPath: string): Promise<void> {
    const config = await prettier.resolveConfig('.');
    for (const entry of fs.readdirSync(dirPath)) {
        const full = path.join(dirPath, entry);
        if (fs.statSync(full).isDirectory()) {
            await formatDirWithPrettier(full);
        } else if (entry.endsWith('.md')) {
            const fileContent = fs.readFileSync(full, 'utf-8');
            const formatted = await prettier.format(fileContent, {...config, parser: 'markdown', filepath: full});
            fs.writeFileSync(full, formatted);
        }
    }
}
