import fs from 'node:fs';
import path from 'node:path';
import {dereference} from '@scalar/openapi-parser';
import type {OpenAPI} from '@scalar/openapi-types';
import type {AstroIntegration} from 'astro';
import * as prettier from 'prettier';

/**
 * Options for the OpenAPI operators plugin.
 * @param input The path to the OpenAPI specification file (JSON).
 * @param outputDir The directory where the generated markdown files will be saved.
 */
interface OpenApiOperatorsOptions {
    input: string;
    outputDir: string;
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

                const {input, outputDir} = options;
                const outputPath = path.resolve(outputDir);

                const operatorsOutputPath = path.join(outputPath, 'operators');
                const plotsOutputPath = path.join(outputPath, 'plots');

                logger.info(`Generating docs from ${input}...`);

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

                    fs.rmSync(operatorsOutputPath, {recursive: true, force: false});
                    fs.rmSync(plotsOutputPath, {recursive: true, force: false});

                    if (!fs.existsSync(operatorsOutputPath)) fs.mkdirSync(operatorsOutputPath, {recursive: true});
                    if (!fs.existsSync(plotsOutputPath)) fs.mkdirSync(plotsOutputPath, {recursive: true});

                    const rasterOperatorSchema: OpenAPI.SchemaObject = openapi.schema?.components?.schemas?.['RasterOperator'];
                    const vectorOperatorSchema: OpenAPI.SchemaObject = openapi.schema?.components?.schemas?.['VectorOperator'];
                    const plotOperatorSchema: OpenAPI.SchemaObject = openapi.schema?.components?.schemas?.['PlotOperator'];

                    const operatorMds: OperatorMd[] = [];
                    const plotOperatorMds: OperatorMd[] = [];

                    for (const operatorPosition in rasterOperatorSchema.oneOf) {
                        const operatorSchema: OpenAPI.SchemaObject = rasterOperatorSchema.oneOf[operatorPosition];
                        const operatorName = operatorSchema['x-reference-id'];
                        if (!operatorName) {
                            throw new Error(`No operator name found for position ${operatorPosition} in RasterOperator`);
                        }
                        operatorMds.push(generateMarkdownForOperator(operatorName, operatorSchema));
                    }

                    for (const operatorPosition in vectorOperatorSchema.oneOf) {
                        const operatorSchema: OpenAPI.SchemaObject = vectorOperatorSchema.oneOf[operatorPosition];
                        const operatorName = operatorSchema['x-reference-id'];
                        if (!operatorName) {
                            throw new Error(`No operator name found for position ${operatorPosition} in VectorOperator`);
                        }
                        operatorMds.push(generateMarkdownForOperator(operatorName, operatorSchema));
                    }

                    for (const operatorPosition in plotOperatorSchema.oneOf) {
                        const operatorSchema: OpenAPI.SchemaObject = plotOperatorSchema.oneOf[operatorPosition];
                        const operatorName = operatorSchema['x-reference-id'];
                        if (!operatorName) {
                            throw new Error(`No operator name found for position ${operatorPosition} in PlotOperator`);
                        }
                        plotOperatorMds.push(generateMarkdownForOperator(operatorName, operatorSchema));
                    }

                    for (const operatorMd in operatorMds) {
                        writeMarkdownFile(path.join(outputDir, 'operators'), operatorMds[operatorMd]);
                        logger.info(`Written file: ${path.join(outputDir, 'operators', operatorMds[operatorMd].filename)}`);
                    }

                    for (const plotOperatorMd in plotOperatorMds) {
                        writeMarkdownFile(path.join(outputDir, 'plots'), plotOperatorMds[plotOperatorMd]);
                        logger.info(`Written file: ${path.join(outputDir, 'plots', plotOperatorMds[plotOperatorMd].filename)}`);
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
    const parametersTable = parametersToMarkdownTable(parseParameters(Object.entries(operatorSchema.properties?.params?.properties)), true);
    const hasSources = !!operatorSchema.properties.sources;
    const sourcesTable = !hasSources
        ? ''
        : parametersToMarkdownTable(parseParameters(Object.entries(operatorSchema.properties?.sources?.properties)), false);
    const examples: string[] = operatorSchema.examples.map((example: JSON) => JSON.stringify(example, null, 2));
    const hasExamples = examples.length > 0;

    return {
        filename: `${operatorId}.md`,
        title: operatorSchema.title ?? operatorId,
        content: `\
${operatorSchema.description}

## Parameters

${parametersTable}

${!hasSources ? '' : `## Sources\n\n${sourcesTable}`}

## Examples

${!hasExamples ? '' : '```json\n' + examples.join('\n```\n\n```json\n') + '\n```'}
`,
    };
}

/**
 * Writes the markdown content for an operator to a file.
 * @param outputDir The directory where the markdown file will be saved.
 * @param operatorMd The markdown content for the operator.
 */
async function writeMarkdownFile(outputDir: string, operatorMd: OperatorMd) {
    const filePath = path.join(outputDir, operatorMd.filename);
    const fileContent = `\
---
title: ${operatorMd.title}
---

${operatorMd.content}`;

    const config = await prettier.resolveConfig(filePath);
    const formatted = await prettier.format(fileContent, {...config, parser: 'markdown', filepath: filePath});

    fs.writeFileSync(filePath, formatted);
}

/**
 * Parses the parameters from the OpenAPI schema and returns an array of parameter entries for markdown generation.
 * It handles both simple parameters and those defined with `oneOf` for multiple types.
 * @param parameters An array of parameter entries from the OpenAPI schema.
 * @returns An array of parameter table entries with name, type, description, and examples.
 */
function parseParameters(parameters: [string, OpenAPI.SchemaObject][]): ParameterTableEntry[] {
    return parameters.map(([name, param]) => {
        const entry = {
            name,
            type: param['x-reference-id'] ?? param.type ?? 'unknown',
            description: param.description ?? '',
            examples: param.examples ? Object.values(param.examples).map((ex) => JSON.stringify(ex, null, 2)) : [],
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
