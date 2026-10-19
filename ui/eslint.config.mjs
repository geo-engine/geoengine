import {defineConfig} from 'eslint/config';
import angular from 'angular-eslint';
import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import eslintConfigPrettier from 'eslint-config-prettier/flat';
import {importX} from 'eslint-plugin-import-x';
import {createTypeScriptImportResolver} from 'eslint-import-resolver-typescript';

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
            'import-x': importX,
        },

        settings: {
            'import-x/resolver-next': [createTypeScriptImportResolver({project: 'tsconfig.json'})],
        },

        languageOptions: {
            parserOptions: {
                projectService: true,
                tsconfigRootDir: import.meta.dirname,
            },
        },

        rules: {
            eqeqeq: ['error', 'smart'],
            'guard-for-in': 'error',
            'no-bitwise': 'error',
            'no-console': [
                'error',
                {
                    allow: ['warn', 'error'],
                },
            ],

            '@angular-eslint/component-selector': ['error', {type: 'element', prefix: 'geoengine', style: 'kebab-case'}],
            '@angular-eslint/directive-selector': [
                'error',
                {
                    type: 'attribute',
                    prefix: 'geoengine',
                    style: 'camelCase',
                },
            ],
            '@angular-eslint/prefer-signals': 'error',
            '@angular-eslint/use-lifecycle-interface': 'error',

            'import-x/order': [
                'error',
                {
                    groups: ['builtin', 'external', 'internal', ['parent', 'sibling', 'index']],
                    pathGroups: [{pattern: '@geoengine/**', group: 'internal'}],
                    pathGroupsExcludedImportTypes: ['builtin'],
                    'newlines-between': 'never',
                    alphabetize: {order: 'asc', caseInsensitive: true},
                },
            ],

            '@typescript-eslint/array-type': 'off',
            '@typescript-eslint/dot-notation': 'off',
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
            '@typescript-eslint/switch-exhaustiveness-check': ['error', {considerDefaultExhaustiveForUnions: true}],
            '@typescript-eslint/unbound-method': ['error', {ignoreStatic: true}],
            '@typescript-eslint/no-inferrable-types': ['error', {ignoreParameters: true, ignoreProperties: true}],
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

// Warnings from the presets are errors, so that the code stays free of warnings.

export default defineConfig([
    ...config,
    {
        files: ['**/*.ts'],
        rules: warningsAsErrors('**/*.ts'),
    },
    {
        files: ['**/*.html'],
        rules: warningsAsErrors('**/*.html'),
    },
]);
