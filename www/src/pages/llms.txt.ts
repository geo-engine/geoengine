import type {APIRoute} from 'astro';
import {getCollection} from 'astro:content';

// Same sections and order as the sidebar in `astro.config.mjs`.
const SECTIONS = [
    ['getting-started', 'Getting Started'],
    ['the-geo-engine', 'Concepts'],
    ['api', 'Interfaces'],
    ['datatypes', 'Datatypes'],
    ['operators', 'Operators'],
    ['plots', 'Plots'],
    ['python', 'Python Library'],
];

/** Index of all docs pages for AI agents, see https://llmstxt.org. Each page is linked as Markdown. */
export const GET: APIRoute = async ({site}) => {
    const docs = await getCollection('docs');

    const section = ([dir, label]: string[]): string => {
        const links = docs
            .filter((entry) => entry.id.startsWith(`docs/${dir}`))
            .sort((a, b) => (a.data.sidebar.order ?? Infinity) - (b.data.sidebar.order ?? Infinity) || a.id.localeCompare(b.id))
            .map((entry) => `- [${entry.data.title}](${new URL(`/${entry.id}.md`, site)})`);
        return `## ${label}\n\n${links.join('\n')}`;
    };

    const text = `# Geo Engine

> Geo Engine is a server for processing and visualizing geospatial raster and vector data with native time-series support.
> Data is combined and transformed in processing graphs, which are JSON trees of operators, and queried through the Python library, the REST API or OGC services.

If you work with Geo Engine for a user, read [Use Geo Engine with an AI Agent](${new URL('/docs/getting-started/ai-agents.md', site)}) first.
Read the page of an operator or plot before you use it; do not guess its parameters.

${SECTIONS.map(section).join('\n\n')}
`;

    return new Response(text, {headers: {'Content-Type': 'text/plain; charset=utf-8'}});
};
