// 1. Import utilities from `astro:content`
import {defineCollection} from 'astro:content';
import {z} from 'astro/zod';

// 2. Import loader(s)
import {glob, file} from 'astro/loaders';
import {docsLoader} from '@astrojs/starlight/loaders';
import {docsSchema} from '@astrojs/starlight/schema';

// 3. Define your collection(s)
const features = defineCollection({
    loader: glob({pattern: '**/*.md', base: './src/data/features'}),
    schema: ({image}) =>
        z.object({
            title: z.string(),
            icon: image(),
        }),
});
const components = defineCollection({
    loader: glob({pattern: '**/*.md', base: './src/data/components'}),
    schema: ({image}) =>
        z.object({
            title: z.string(),
            subtitle: z.string(),
            icon: image(),
            links: z.array(
                z.object({
                    title: z.string(),
                    url: z.string(),
                }),
            ),
        }),
});
const docs = defineCollection({loader: docsLoader(), schema: docsSchema()});
const openapi_types = defineCollection({
    loader: file('../openapi.json', {
        parser: async (text) => {
            const data = JSON.parse(text);
            return data.components.schemas;
        },
    }),
    schema: z.object({
        title: z.string().optional(),
        description: z.string().optional(),
        properties: z
            .looseObject({
                type: z
                    .object({
                        enum: z.array(z.string()).optional(),
                    })
                    .optional(),
                params: z
                    .object({
                        $ref: z.string().optional(),
                    })
                    .optional(),
                sources: z
                    .object({
                        $ref: z.string().optional(),
                    })
                    .optional(),
            })
            .optional(),
        examples: z.array(z.json({})).optional(),
    }),
});

// 4. Export a single `collections` object to register your collection(s)
export const collections = {features, components, docs, 'openapi-types': openapi_types};
