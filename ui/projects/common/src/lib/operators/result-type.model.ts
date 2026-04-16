/**
 * Represents the result of an operator.
 */
export abstract class ResultType {
    abstract readonly code: string;

    /**
     * Human-readable form of the result type.
     */
    toString(): string {
        return this.code.charAt(0).toUpperCase() + this.code.substring(1);
    }
}

class Raster extends ResultType {
    code = 'raster';
}

class Points extends ResultType {
    code = 'points';
}

class Lines extends ResultType {
    code = 'lines';
}

class Polygons extends ResultType {
    code = 'polygons';
}

class Data extends ResultType {
    code = 'data';
}

class Plot extends ResultType {
    code = 'plot';
}

class Text extends ResultType {
    code = 'text';
}

export class ResultTypeCollection {
    static readonly INSTANCE = new ResultTypeCollection();

    RASTER: ResultType = new Raster();
    POINTS: ResultType = new Points();
    LINES: ResultType = new Lines();
    POLYGONS: ResultType = new Polygons();
    DATA: ResultType = new Data();
    PLOT: ResultType = new Plot();
    TEXT: ResultType = new Text();

    ALL_TYPES: Array<ResultType>;
    INPUT_TYPES: Array<ResultType>;
    VECTOR_TYPES: Array<ResultType>;
    LAYER_TYPES: Array<ResultType>;
    PLOT_TYPES: Array<ResultType>;

    protected constructor() {
        this.ALL_TYPES = [this.RASTER, this.POINTS, this.LINES, this.POLYGONS, this.DATA, this.PLOT, this.TEXT];

        this.INPUT_TYPES = [this.RASTER, this.POINTS, this.LINES, this.POLYGONS, this.DATA];

        this.VECTOR_TYPES = [this.POINTS, this.LINES, this.POLYGONS];

        this.LAYER_TYPES = [this.RASTER, this.POINTS, this.LINES, this.POLYGONS];

        this.PLOT_TYPES = [this.PLOT, this.TEXT];
    }

    fromCode(type: string): ResultType {
        switch (type.toLowerCase()) {
            case this.RASTER.code:
                return this.RASTER;
            case this.POINTS.code:
            case 'point':
                return this.POINTS;
            case 'line string':
            case 'multi line string':
            case this.LINES.code:
                return this.LINES;
            case this.POLYGONS.code:
            case 'multi surface':
            case 'multi polygon':
                return this.POLYGONS;
            case this.PLOT.code:
                return this.PLOT;
            case this.TEXT.code:
                return this.TEXT;
            default:
                throw new Error('Invalid Result Type: ' + type);
        }
    }
}

// eslint-disable-next-line @typescript-eslint/naming-convention
export const ResultTypes = ResultTypeCollection.INSTANCE;
