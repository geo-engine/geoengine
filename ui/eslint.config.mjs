import {defineConfig} from 'eslint/config';
import preferArrow from 'eslint-plugin-prefer-arrow-functions';
import angular from 'angular-eslint';
import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import eslintConfigPrettier from 'eslint-config-prettier/flat';

const config = defineConfig([
    {
        files: ['**/*.ts'],

        extends: [
            eslint.configs.recommended,
            tseslint.configs.recommendedTypeChecked,
            tseslint.configs.stylisticTypeChecked,
            angular.configs.tsRecommended,
            eslintConfigPrettier,
        ],

        processor: angular.processInlineTemplates,

        plugins: {
            'prefer-arrow': preferArrow,
        },

        languageOptions: {
            parser: tseslint.parser,

            parserOptions: {
                project: ['tsconfig.json'],
                createDefaultProgram: false,
            },
        },

        rules: {
            'guard-for-in': 'error',
            'id-blacklist': 'off',
            'id-match': 'off',
            'import/order': 'off',
            'no-redeclare': ['error', {builtinGlobals: false}],
            'no-underscore-dangle': 'off',
            'valid-typeof': 'error',
            'no-bitwise': 'error',
            'no-empty-function': 'error',
            'no-unused-vars': 'off',
            'no-shadow': 'off',
            'no-console': [
                'error',
                {
                    allow: ['warn', 'error'],
                },
            ],
            camelcase: 'off',

            '@angular-eslint/component-selector': ['error', {type: 'element', prefix: 'geoengine', style: 'kebab-case'}],
            '@angular-eslint/directive-selector': [
                'error',
                {
                    type: 'attribute',
                    prefix: 'geoengine',
                    style: 'camelCase',
                },
            ],
            '@angular-eslint/prefer-signals': 'warn',
            '@angular-eslint/use-lifecycle-interface': 'error',

            '@typescript-eslint/array-type': ['off', {default: 'generic'}],
            '@typescript-eslint/consistent-type-definitions': 'error',
            '@typescript-eslint/dot-notation': 'off',
            '@typescript-eslint/explicit-member-accessibility': [
                'off',
                {
                    accessibility: 'explicit',
                },
            ],
            '@typescript-eslint/member-ordering': [
                'error',
                {
                    default: {
                        memberTypes: ['signature', 'field', 'static-initialization', 'constructor', 'accessor', ['get', 'set', 'method']],
                    },
                },
            ],
            '@typescript-eslint/naming-convention': [
                'error',
                {
                    selector: 'default',
                    format: ['camelCase'],
                },
                {
                    selector: 'import',
                    format: ['camelCase', 'PascalCase'],
                },
                {
                    selector: 'variable',
                    format: ['camelCase', 'UPPER_CASE'],
                    leadingUnderscore: 'allow',
                },
                {
                    selector: 'parameter',
                    format: ['camelCase'],
                    leadingUnderscore: 'allow',
                },
                {
                    selector: 'parameterProperty',
                    format: ['camelCase'],
                    leadingUnderscore: 'allow',
                },
                {
                    selector: 'memberLike',
                    modifiers: ['private'],
                    format: ['camelCase'],
                    leadingUnderscore: 'allow',
                },
                {
                    selector: 'property',
                    format: ['camelCase', 'UPPER_CASE', 'PascalCase'],
                    leadingUnderscore: 'allow',
                },
                {
                    selector: 'enumMember',
                    format: ['PascalCase', 'UPPER_CASE'],
                    leadingUnderscore: 'allow',
                },
                {
                    selector: 'accessor',
                    format: ['camelCase', 'UPPER_CASE'],
                },
                {
                    selector: 'typeLike',
                    format: ['PascalCase'],
                },
            ],
            '@typescript-eslint/no-unused-vars': [
                'error',
                {
                    argsIgnorePattern: '^_',
                    caughtErrorsIgnorePattern: '^_',
                    varsIgnorePattern: '^_',
                },
            ],
            '@typescript-eslint/no-shadow': 'error',
            '@typescript-eslint/explicit-function-return-type': 'error',
            '@typescript-eslint/no-unsafe-argument': 'warn', // TODO: type lineage-graph, map-container and time-slider in core
            '@typescript-eslint/no-unsafe-assignment': 'warn', // TODO: type lineage-graph, map-container and time-slider in core
            '@typescript-eslint/no-unsafe-call': 'warn', // TODO: type lineage-graph, map-container and time-slider in core
            '@typescript-eslint/no-unsafe-member-access': 'warn', // TODO: type lineage-graph, map-container and time-slider in core
            '@typescript-eslint/no-unsafe-return': 'error',
            '@typescript-eslint/unbound-method': ['error', {ignoreStatic: true}],
            '@typescript-eslint/no-floating-promises': 'error',
            '@typescript-eslint/no-inferrable-types': ['error', {ignoreParameters: true, ignoreProperties: true}],

            'prefer-arrow/prefer-arrow-functions': [
                'error',
                {
                    singleReturnOnly: true,
                },
            ],
        },
    },
    {
        files: ['**/*.html'],

        extends: [angular.configs.templateRecommended, eslintConfigPrettier],

        rules: {},
    },
]);

const isWarning = (entry) => {
    const severity = Array.isArray(entry) ? entry[0] : entry;
    return severity === 'warn' || severity === 1;
};

/**
 * Collect the effective rules of all config objects for the given file glob (later objects win)
 * and set every rule that ends up as a warning to an error, keeping its options.
 */
const warningsAsErrors = (filesGlob) => {
    const rules = {};
    for (const {files, rules: configRules} of config) {
        if (JSON.stringify(files ?? []).includes(JSON.stringify(filesGlob))) {
            Object.assign(rules, configRules);
        }
    }
    return Object.fromEntries(
        Object.entries(rules)
            .filter(([, entry]) => isWarning(entry))
            .map(([rule, entry]) => [rule, Array.isArray(entry) ? ['error', ...entry.slice(1)] : 'error']),
    );
};

// Warnings are errors everywhere except in the `core` library, which still has legacy warnings to fix.
// Once a rule has no warnings left in `core`, set it to 'error' above.
const withLegacyWarnings = ['projects/core/**'];

export default defineConfig([
    ...config,
    {
        files: ['**/*.ts'],
        ignores: withLegacyWarnings,
        rules: warningsAsErrors('**/*.ts'),
    },
    {
        files: ['**/*.html'],
        ignores: withLegacyWarnings,
        rules: warningsAsErrors('**/*.html'),
    },
]);
