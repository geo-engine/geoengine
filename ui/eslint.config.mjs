import {defineConfig} from 'eslint/config';
import preferArrow from 'eslint-plugin-prefer-arrow';
import angular from 'angular-eslint';
import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import eslintConfigPrettier from 'eslint-config-prettier/flat';

export default defineConfig([
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
            'arrow-parens': ['off', 'always'],
            'brace-style': ['off', '1tbs'], // prettier is currently inconsistent, re-enable if possible
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
            '@typescript-eslint/no-unsafe-argument': 'warn', // TODO: use typed forms
            '@typescript-eslint/no-unsafe-assignment': 'warn', // TODO: use typed forms
            '@typescript-eslint/no-unsafe-call': 'warn', // TODO: use typed forms
            '@typescript-eslint/no-unsafe-member-access': 'warn', // TODO: use typed forms
            '@typescript-eslint/no-unsafe-return': 'warn', // TODO: use typed forms
            '@typescript-eslint/unbound-method': ['error', {ignoreStatic: true}],
            '@typescript-eslint/no-floating-promises': 'warn', // TODO: fix promises
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
