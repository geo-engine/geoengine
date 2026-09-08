import fs from 'node:fs';
import path from 'node:path';
import {dereference} from '@scalar/openapi-parser';
import type {OpenAPI} from '@scalar/openapi-types';
import type {AstroIntegration, AstroIntegrationLogger} from 'astro';
import * as prettier from 'prettier';

/**
 * Options for the OpenAPI operators plugin.
 * @param input The path to the OpenAPI specification file (JSON).
 * @param operatorsOutputDir The directory where the generated markdown files for operators will be saved.
 * @param plotsOutputDir The directory where the generated markdown files for plot operators will be saved.
 */
interface OpenApiOperatorsOptions {
    input: string;
    operatorsOutputDir: string;
    plotsOutputDir: string;
}

/**
 * Astro integration that generates markdown documentation for operators defined in an OpenAPI spec.
 * It reads the OpenAPI spec, extracts operator definitions, and writes markdown files for each operator
 * in the specified output directory.
 * @param options Options for the OpenAPI operators plugin.
 * @returns An Astro integration that fires on hook `config:setup`
 */
export default function openApiOperatorsPlugin(options: OpenApiOperatorsOptions): AstroIntegration {
    return {
        name: 'openapi-operators-to-markdown',
        hooks: {
            'astro:config:setup': async ({command: _, isRestart, logger}) => {
                // Prevent double-running during simple dev reloads
                if (isRestart) return;

                const {input, operatorsOutputDir, plotsOutputDir} = options;

                logger.info(`Generating operator and plot docs from ${input}...`);

                try {
                    const openapi = dereference(fs.readFileSync(input, 'utf-8'), {
                        onDereference: ({schema, ref}) => {
                            // Inject the full reference path or just the ID
                            // ref: "#/components/parameters/userId"
                            if (schema && typeof schema === 'object') {
                                schema['x-reference-id'] = ref.split('/').pop();
                                schema['x-full-ref'] = ref;
                            }
                        },
                    });

                    clearDirButKeepIndex(operatorsOutputDir);
                    clearDirButKeepIndex(plotsOutputDir);

                    const rasterOperatorSchema: OpenAPI.SchemaObject = openapi.schema?.components?.schemas?.['RasterOperator'];
                    const vectorOperatorSchema: OpenAPI.SchemaObject = openapi.schema?.components?.schemas?.['VectorOperator'];
                    const plotOperatorSchema: OpenAPI.SchemaObject = openapi.schema?.components?.schemas?.['PlotOperator'];

                    const operatorMds: OperatorMd[] = [];
                    const plotOperatorMds: OperatorMd[] = [];

                    const operatorGroups = [
                        {schema: rasterOperatorSchema, dest: operatorMds, label: 'RasterOperator'},
                        {schema: vectorOperatorSchema, dest: operatorMds, label: 'VectorOperator'},
                        {schema: plotOperatorSchema, dest: plotOperatorMds, label: 'PlotOperator'},
                    ];

                    for (const group of operatorGroups) {
                        const {schema, dest, label} = group as {schema?: OpenAPI.SchemaObject; dest: OperatorMd[]; label: string};
                        if (!schema) {
                            continue;
                        }

                        const oneOf = schema.oneOf ?? [];
                        for (const operatorPosition in oneOf) {
                            const operatorSchema = oneOf[operatorPosition] as OpenAPI.SchemaObject | undefined;
                            if (!operatorSchema) {
                                continue;
                            }

                            const operatorName = operatorSchema['x-reference-id'];
                            if (!operatorName) {
                                throw new Error(`No operator name found for position ${operatorPosition} in ${label}`);
                            }
                            dest.push(generateMarkdownForOperator(operatorName, operatorSchema));
                        }
                    }

                    for (const operatorMd of operatorMds) {
                        await writeMarkdownFile(operatorsOutputDir, operatorMd, logger);
                    }

                    for (const plotOperatorMd of plotOperatorMds) {
                        await writeMarkdownFile(plotsOutputDir, plotOperatorMd, logger);
                    }

                    logger.info('✅ Documentation generated successfully.');
                } catch (error) {
                    logger.error(`Failed to generate docs: ${error}`);
                }
            },
        },
    };
}

/**
 * Represents the markdown content for an operator, including the filename, title, and content.
 */
interface OperatorMd {
    filename: string;
    title: string;
    content: string;
}

/**
 * Generates markdown content for a given operator schema from the OpenAPI specification.
 * It extracts parameters, sources, and examples to create a structured markdown document.
 * @param operatorId The identifier for the operator (used for filename and title).
 * @param operatorSchema The OpenAPI schema object representing the operator.
 * @returns An object containing the filename, title, and markdown content for the operator.
 */
function generateMarkdownForOperator(operatorId: string, operatorSchema: OpenAPI.SchemaObject): OperatorMd {
    const paramsProperties = operatorSchema.properties?.params?.properties ?? {};
    const sourceProperties = operatorSchema.properties?.sources?.properties ?? {};

    const parametersTable = parametersToMarkdownTable(parseParameters(Object.entries(paramsProperties)), true);
    const hasSources = !!operatorSchema.properties?.sources;
    const sourcesTable = !hasSources ? '' : parametersToMarkdownTable(parseParameters(Object.entries(sourceProperties)), false);

    const rawExamples = Array.isArray(operatorSchema.examples)
        ? operatorSchema.examples
        : operatorSchema.examples && typeof operatorSchema.examples === 'object'
          ? Object.values(operatorSchema.examples as Record<string, unknown>)
          : [];
    const singleExample = operatorSchema.example !== undefined ? [operatorSchema.example] : [];
    const examples: string[] = [...rawExamples, ...singleExample].map((example: unknown) => JSON.stringify(example, null, 2));
    const hasExamples = examples.length > 0;
    const description = normalizeMarkdownLinks(operatorSchema.description ?? '');

    return {
        filename: `${operatorId}.md`,
        title: operatorSchema.title ?? operatorId,
        content: `\
${description}

## Parameters

${parametersTable}

${!hasSources ? '' : `## Sources\n\n${sourcesTable}`}

## Examples

${!hasExamples ? '' : '```json\n' + examples.join('\n```\n\n```json\n') + '\n```'}
`,
    };
}

function normalizeMarkdownLinks(markdown: string): string {
    return markdown
        .replace(/\]\((?:\.{0,2}\/)?datatypes\/([^)#]+)(?:\.md)?(?:#([^)]+))?\)/g, (_, name: string, hash?: string) => {
            const target = `/docs/datatypes/${name}`;
            return `](${target}${hash ? `#${hash}` : ''})`;
        })
        .replace(/\]\((?:\/)?datatypes\/([^)#]+)\.html(?:#([^)]+))?\)/g, (_, name: string, hash?: string) => {
            const target = `/docs/datatypes/${name}`;
            return `](${target}${hash ? `#${hash}` : ''})`;
        });
}

/**
 * Writes the markdown content for an operator to a file.
 * @param outputDir The directory where the markdown file will be saved.
 * @param operatorMd The markdown content for the operator.
 * @param logger The logger instance for logging messages.
 */
async function writeMarkdownFile(outputDir: string, operatorMd: OperatorMd, logger: AstroIntegrationLogger): Promise<void> {
    const filePath = path.join(outputDir, operatorMd.filename);
    const fileContent = `\
---
title: ${operatorMd.title}
---

${operatorMd.content}`;

    const config = await prettier.resolveConfig(filePath);
    const formatted = await prettier.format(fileContent, {...config, parser: 'markdown', filepath: filePath});

    fs.writeFileSync(filePath, formatted);
    logger.info(`Written file: ${filePath}`);
}

/**
 * Parses the parameters from the OpenAPI schema and returns an array of parameter entries for markdown generation.
 * It handles both simple parameters and those defined with `oneOf` for multiple types.
 * @param parameters An array of parameter entries from the OpenAPI schema.
 * @returns An array of parameter table entries with name, type, description, and examples.
 */
function parseParameters(parameters: [string, OpenAPI.SchemaObject][]): ParameterTableEntry[] {
    return parameters.map(([name, param]) => {
        const exampleValues = Array.isArray(param.examples)
            ? param.examples
            : param.examples && typeof param.examples === 'object'
              ? Object.values(param.examples as Record<string, unknown>)
              : [];
        const singleExample = param.example !== undefined ? [param.example] : [];

        const entry = {
            name,
            type: param['x-reference-id'] ?? param.type ?? 'unknown',
            description: param.description ?? '',
            examples: [...exampleValues, ...singleExample].map((ex) => JSON.stringify(ex, null, 2)),
        };
        if (entry.type === 'unknown' && param.oneOf) {
            entry.type = param.oneOf
                .map((schema: OpenAPI.SchemaObject) => schema['x-reference-id'] ?? schema.type ?? 'unknown')
                .join(' or ');
        }
        return entry;
    });
}

/**
 * Converts an array of parameter entries into a markdown table format.
 * @param parameters An array of parameter table entries to be included in the markdown table.
 * @param withExamples A boolean indicating whether to include an "Examples" column in the table.
 * @returns A string representing the markdown table for the given parameters.
 */
function parametersToMarkdownTable(parameters: ParameterTableEntry[], withExamples: boolean): string {
    if (parameters.length === 0) {
        return 'No parameters.';
    }

    let header = '| Name | Type | Description |';
    if (withExamples) header += ' Examples |';
    header += '\n';
    header += '| --- | --- | --- |';
    if (withExamples) header += '  --- |';
    header += '\n';

    const rows = parameters
        .map((param) => {
            let row = `| ${param.name} | ${param.type} | ${param.description.replace(/\n/g, '<br>')} |`;
            if (withExamples) {
                const examplesFormatted = param.examples.map((ex) => `\`${ex}\``).join('<br>');
                row += ` ${examplesFormatted} |`;
            }
            return row;
        })
        .join('\n');

    return header + rows;
}

/**
 * Represents an entry in the parameter table, including the parameter name, type, description, and examples.
 */
interface ParameterTableEntry {
    name: string;
    type: string;
    description: string;
    examples: string[];
}

/**
 * Represents the markdown content for an operator, including the filename, title, and content.
 */
interface OperatorMd {
    filename: string;
    title: string;
    content: string;
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
