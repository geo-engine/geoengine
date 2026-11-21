import type {APIRoute, GetStaticPaths} from 'astro';
import {getCollection} from 'astro:content';

/** Serves every docs page as Markdown next to its HTML page, e.g. `/docs/operators/expression.md`, for AI agents. */
export const getStaticPaths: GetStaticPaths = async () =>
    (await getCollection('docs')).map((entry) => ({params: {slug: entry.id}, props: {entry}}));

export const GET: APIRoute = ({props}) =>
    new Response(`# ${props.entry.data.title}\n\n${props.entry.body}`, {
        headers: {'Content-Type': 'text/markdown; charset=utf-8'},
    });
