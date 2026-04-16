import {ResultType, ResultTypes} from './result-type.model';

/**
 * A class about a raster data type.
 */
export abstract class RasterDataType {
    /**
     * Create a human readable output of the data type.
     *
     * @returns The name.
     */
    abstract toString(): string;

    /**
     * @return The name of the data type.
     */
    abstract getCode(): string;

    /**
     * @return the largest value.
     */
    abstract getMin(): number;

    /**
     * @return the smallest value.
     */
    abstract getMax(): number;
}

class Byte extends RasterDataType {
    toString(): string {
        return 'Byte';
    }

    getCode(): string {
        return 'U8';
    }

    getMin(): number {
        return 0;
    }

    getMax(): number {
        return 255;
    }
}

class Int16 extends RasterDataType {
    toString(): string {
        return 'Int 16';
    }

    getCode(): string {
        return 'I16';
    }

    getMin(): number {
        return -32768;
    }

    getMax(): number {
        return 32767;
    }
}

class UInt16 extends RasterDataType {
    toString(): string {
        return 'Unsigned Int 16';
    }

    getCode(): string {
        return 'U16';
    }

    getMin(): number {
        return 0;
    }

    getMax(): number {
        return 65535;
    }
}

class Int32 extends RasterDataType {
    toString(): string {
        return 'Int 32';
    }

    getCode(): string {
        return 'I32';
    }

    getMin(): number {
        return -2147483648;
    }

    getMax(): number {
        return 2147483647;
    }
}

class UInt32 extends RasterDataType {
    toString(): string {
        return 'Unsigned Int 32';
    }

    getCode(): string {
        return 'U32';
    }

    getMin(): number {
        return 0;
    }

    getMax(): number {
        return 4294967295;
    }
}

class Float32 extends RasterDataType {
    toString(): string {
        return 'Float 32';
    }

    getCode(): string {
        return 'F32';
    }

    getMin(): number {
        return -Number.MAX_VALUE;
    }

    getMax(): number {
        return Number.MAX_VALUE;
    }
}

class Float64 extends RasterDataType {
    toString(): string {
        return 'Float 64';
    }

    getCode(): string {
        return 'F64';
    }

    getMin(): number {
        return -Number.MAX_VALUE;
    }

    getMax(): number {
        return Number.MAX_VALUE;
    }
}

export class RasterDataTypeCollection {
    static readonly INSTANCE = new RasterDataTypeCollection();

    Byte: RasterDataType = new Byte();
    Int16: RasterDataType = new Int16();
    UInt16: RasterDataType = new UInt16();
    Int32: RasterDataType = new Int32();
    UInt32: RasterDataType = new UInt32();
    Float32: RasterDataType = new Float32();
    Float64: RasterDataType = new Float64();

    ALL_DATATYPES: Array<RasterDataType>;

    protected constructor() {
        this.ALL_DATATYPES = [this.Byte, this.Int16, this.UInt16, this.Int32, this.UInt32, this.Float32, this.Float64];
    }

    fromCode(code: string): RasterDataType {
        switch (code) {
            case this.Byte.getCode():
                return this.Byte;
            case this.Int16.getCode():
                return this.Int16;
            case this.UInt16.getCode():
                return this.UInt16;
            case this.Int32.getCode():
                return this.Int32;
            case this.UInt32.getCode():
                return this.UInt32;
            case this.Float32.getCode():
                return this.Float32;
            case this.Float64.getCode():
                return this.Float64;
            default:
                throw new Error(`Invalid Data Type: ${code}`);
        }
    }
}

// eslint-disable-next-line @typescript-eslint/naming-convention
export const RasterDataTypes = RasterDataTypeCollection.INSTANCE;

export abstract class VectorDataType {
    abstract readonly resultType: ResultType;

    /**
     * Create a human readable output of the data type.
     *
     * @returns The name.
     */
    toString(): string {
        return this.getCode();
    }

    /**
     * @return The name of the data type.
     */
    abstract getCode(): 'Data' | 'MultiPoint' | 'MultiLineString' | 'MultiPolygon';
}

class Data extends VectorDataType {
    resultType = ResultTypes.DATA;

    getCode(): 'Data' | 'MultiPoint' | 'MultiLineString' | 'MultiPolygon' {
        return 'Data';
    }
}

class MultiPoint extends VectorDataType {
    resultType = ResultTypes.POINTS;

    getCode(): 'Data' | 'MultiPoint' | 'MultiLineString' | 'MultiPolygon' {
        return 'MultiPoint';
    }
}

class MultiLineString extends VectorDataType {
    resultType = ResultTypes.LINES;

    getCode(): 'Data' | 'MultiPoint' | 'MultiLineString' | 'MultiPolygon' {
        return 'MultiLineString';
    }
}

class MultiPolygon extends VectorDataType {
    resultType = ResultTypes.POLYGONS;

    getCode(): 'Data' | 'MultiPoint' | 'MultiLineString' | 'MultiPolygon' {
        return 'MultiPolygon';
    }
}

export class VectorDataTypeCollection {
    static readonly INSTANCE = new VectorDataTypeCollection();

    /* eslint-disable @typescript-eslint/naming-convention */
    Data: VectorDataType = new Data();
    MultiPoint: VectorDataType = new MultiPoint();
    MultiLineString: VectorDataType = new MultiLineString();
    MultiPolygon: VectorDataType = new MultiPolygon();

    fromCode(code: string): VectorDataType {
        switch (code) {
            case this.Data.getCode():
                return this.Data;
            case this.MultiPoint.getCode():
                return this.MultiPoint;
            case this.MultiLineString.getCode():
                return this.MultiLineString;
            case this.MultiPolygon.getCode():
                return this.MultiPolygon;
            default:
                throw new Error(`Invalid Data Type: ${code}`);
        }
    }
}

export const VectorDataTypes = VectorDataTypeCollection.INSTANCE;

export abstract class VectorColumnDataType {
    /**
     * @return The name of the data type.
     */
    abstract readonly code: string;

    /**
     * @return Is this type a numeric data type?
     */
    abstract readonly isNumeric: boolean;

    /**
     * Create a human readable output of the data type.
     *
     * @returns The name.
     */
    toString(): string {
        return this.code;
    }
}

class FloatColumn extends VectorColumnDataType {
    readonly code = 'float';
    readonly isNumeric = true;
}

class IntColumn extends VectorColumnDataType {
    readonly code = 'int';
    readonly isNumeric = true;
}

class TextColumn extends VectorColumnDataType {
    readonly code = 'text';
    readonly isNumeric = false;
}

class CategoryColumn extends VectorColumnDataType {
    readonly code = 'category';
    readonly isNumeric = false;
}

class MediaColumn extends VectorColumnDataType {
    readonly code = 'media';
    readonly isNumeric = false;
}

class DateTimeColumn extends VectorColumnDataType {
    readonly code = 'dateTime';
    readonly isNumeric = false;
}

export class VectorColumnDataTypeCollection {
    static readonly INSTANCE = new VectorColumnDataTypeCollection();

    /* eslint-disable @typescript-eslint/naming-convention */
    readonly Float: VectorColumnDataType = new FloatColumn();
    readonly Int: VectorColumnDataType = new IntColumn();
    readonly Text: VectorColumnDataType = new TextColumn();
    readonly Category: VectorColumnDataType = new CategoryColumn();
    readonly Media: VectorColumnDataType = new MediaColumn();
    readonly DateTime: VectorColumnDataType = new DateTimeColumn();

    fromCode(code: string): VectorColumnDataType {
        switch (code) {
            case this.Float.code:
                return this.Float;
            case this.Int.code:
                return this.Int;
            case this.Text.code:
                return this.Text;
            case this.Category.code:
                return this.Category;
            case this.Media.code:
                return this.Media;
            case this.DateTime.code:
                return this.DateTime;
            default:
                throw new Error(`Invalid Column Data Type: ${code}`);
        }
    }
}

export const VectorColumnDataTypes = VectorColumnDataTypeCollection.INSTANCE;
