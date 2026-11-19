/**
 * Keywords of the expression language, compared case-insensitively.
 * They cannot be used as variable names.
 *
 * Keep in sync with `KEYWORDS` in `geoengine/expression/src/lib.rs`.
 */
export const EXPRESSION_KEYWORDS: ReadonlyArray<string> = ['if', 'else', 'let', 'nodata', 'is', 'true', 'false'];

/**
 * Words that are reserved for future language features, compared case-insensitively.
 * They cannot be used as variable names.
 *
 * Keep in sync with `RESERVED_KEYWORDS` in `geoengine/expression/src/lib.rs`.
 */
export const EXPRESSION_RESERVED_KEYWORDS: ReadonlyArray<string> = [
    'and',
    'or',
    'not',
    'in',
    'for',
    'while',
    'loop',
    'break',
    'continue',
    'return',
    'fn',
    'match',
];

/**
 * A function that can be called in a raster expression.
 */
export interface ExpressionFunction {
    readonly name: string;
    readonly signature: string;
    readonly description: string;
}

/**
 * The functions that can be called in a raster expression.
 *
 * Keep in sync with the numeric functions in `geoengine/expression/src/functions.rs`.
 */
export const EXPRESSION_FUNCTIONS: ReadonlyArray<ExpressionFunction> = [
    {name: 'abs', signature: 'abs(a)', description: 'absolute value'},
    {name: 'min', signature: 'min(a, b)', description: 'minimum value'},
    {name: 'max', signature: 'max(a, b)', description: 'maximum value'},
    {name: 'pow', signature: 'pow(a, b)', description: 'a to the power of b'},
    {name: 'mod', signature: 'mod(a, b)', description: 'division remainder'},
    {name: 'sqrt', signature: 'sqrt(a)', description: 'square root'},
    {name: 'ln', signature: 'ln(a)', description: 'natural logarithm'},
    {name: 'log10', signature: 'log10(a)', description: 'base 10 logarithm'},
    {name: 'cos', signature: 'cos(a)', description: 'cosine'},
    {name: 'sin', signature: 'sin(a)', description: 'sine'},
    {name: 'tan', signature: 'tan(a)', description: 'tangent'},
    {name: 'acos', signature: 'acos(a)', description: 'arccosine'},
    {name: 'asin', signature: 'asin(a)', description: 'arcsine'},
    {name: 'atan', signature: 'atan(a)', description: 'arctangent'},
    {name: 'tanh', signature: 'tanh(a)', description: 'hyperbolic tangent'},
    {name: 'round', signature: 'round(a)', description: 'round to the nearest integer'},
    {name: 'ceil', signature: 'ceil(a)', description: 'round up'},
    {name: 'floor', signature: 'floor(a)', description: 'round down'},
    {name: 'to_degrees', signature: 'to_degrees(a)', description: 'convert radians to degrees'},
    {name: 'to_radians', signature: 'to_radians(a)', description: 'convert degrees to radians'},
    {name: 'pi', signature: 'pi()', description: 'the constant π'},
    {name: 'e', signature: 'e()', description: 'the constant e'},
];

const FUNCTION_PREFIX = 'expression_fn_';

/**
 * Adapts a band name to a variable name: replaces every character that is not a letter or a digit by an underscore.
 * Prepends an underscore if the name is empty or starts with a digit.
 *
 * Mirrors `canonicalize_name` in `geoengine/operators/src/processing/expression/mod.rs`.
 */
export function canonicalizeBandName(name: string): string {
    const characters = Array.from(name);
    const prefix = characters.length === 0 || /^\p{N}$/u.test(characters[0]) ? '_' : '';
    return prefix + characters.map((c) => (/^[\p{L}\p{N}]$/u.test(c) ? c : '_')).join('');
}

/**
 * Checks if a name can be written as a variable in an expression.
 *
 * Mirrors `is_valid_variable_name` in `geoengine/expression/src/lib.rs`.
 */
export function isValidExpressionVariable(name: string): boolean {
    if (!/^[A-Za-z][A-Za-z0-9_]*$/.test(name) || name.startsWith(FUNCTION_PREFIX)) {
        return false;
    }

    const lowerCaseName = name.toLowerCase();
    return !EXPRESSION_KEYWORDS.includes(lowerCaseName) && !EXPRESSION_RESERVED_KEYWORDS.includes(lowerCaseName);
}

/**
 * Determines the variable names of raster bands in an expression.
 *
 * A band is referred to by its adapted name if it is a valid variable name and unique among the bands.
 * Otherwise, it is referred to by its letter (`A` for the first band, `B` for the second, …).
 */
export function expressionBandVariables(bandNames: ReadonlyArray<string>): Array<string> {
    const canonicalNames = bandNames.map(canonicalizeBandName);

    return canonicalNames.map((name, index) => {
        const isUnique = canonicalNames.filter((other) => other === name).length === 1;
        if (isValidExpressionVariable(name) && isUnique) {
            return name;
        }
        return String.fromCharCode('A'.charCodeAt(0) + index);
    });
}
