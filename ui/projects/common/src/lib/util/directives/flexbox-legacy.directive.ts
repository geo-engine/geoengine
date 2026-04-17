/* These are legacy directives for angular flex layout */
/* eslint-disable @angular-eslint/directive-selector */

import {Directive, ElementRef, OnChanges, inject, input} from '@angular/core';

@Directive({
    selector: '[fxFlex]',
})
export class FxFlexDirective implements OnChanges {
    private el = inject(ElementRef);

    readonly fxFlex = input<'0.5rem' | '1rem' | '4rem' | 'grow' | ''>('');

    ngOnChanges(): void {
        const fxFlex = this.fxFlex();
        if (fxFlex === '') {
            this.el.nativeElement.style.flex = '1 1 0%';
        } else if (fxFlex === 'grow') {
            this.el.nativeElement.style.flex = '1 1 100%';
        } else if (fxFlex === '0.5rem') {
            this.el.nativeElement.style.flex = '0.5rem';
            this.el.nativeElement.style.maxWidth = '0.5rem';
            this.el.nativeElement.style.minWidth = '0.5rem';
        } else if (fxFlex === '1rem') {
            this.el.nativeElement.style.flex = '1rem';
            this.el.nativeElement.style.maxWidth = '1rem';
            this.el.nativeElement.style.minWidth = '1rem';
        }
    }
}

@Directive({
    selector: '[fxLayout]',
})
export class FxLayoutDirective implements OnChanges {
    private el = inject(ElementRef);

    readonly fxLayout = input<'row' | 'row wrap' | 'column' | 'row-reverse'>('row');

    ngOnChanges(): void {
        this.el.nativeElement.style.display = 'flex';

        switch (this.fxLayout()) {
            case 'row':
                this.el.nativeElement.style.flexDirection = 'row';
                break;
            case 'row wrap':
                this.el.nativeElement.style.flexDirection = 'row';
                this.el.nativeElement.style.flexWrap = 'wrap';
                break;
            case 'row-reverse':
                this.el.nativeElement.style.flexDirection = 'row-reverse';
                break;
            case 'column':
                this.el.nativeElement.style.flexDirection = 'column';
                break;
        }
    }
}

@Directive({
    selector: '[fxLayoutGap]',
})
export class FxLayoutGapDirective implements OnChanges {
    private el = inject(ElementRef);

    readonly fxLayoutGap = input<'0.5rem' | '1rem'>('1rem');

    ngOnChanges(): void {
        switch (this.fxLayoutGap()) {
            case '0.5rem':
                this.el.nativeElement.style.columnGap = '0.5rem';
                break;
            case '1rem':
                this.el.nativeElement.style.columnGap = '1rem';
                break;
        }
    }
}

@Directive({
    selector: '[fxLayoutAlign]',
})
export class FxLayoutAlignDirective implements OnChanges {
    private el = inject(ElementRef);

    readonly fxLayoutAlign = input<
        | 'start center'
        | 'center center'
        | 'space-between start'
        | 'space-between stretch'
        | 'space-between center'
        | 'space-between none'
        | 'space-between baseline'
        | 'center start'
        | 'space-around'
        | 'end'
        | 'start stretch'
    >('start center');

    ngOnChanges(): void {
        switch (this.fxLayoutAlign()) {
            case 'start center':
                this.el.nativeElement.style.justifyContent = 'flex-start';
                this.el.nativeElement.style.alignItems = 'center';
                break;
            case 'center center':
                this.el.nativeElement.style.justifyContent = 'center';
                this.el.nativeElement.style.alignItems = 'center';
                break;
            case 'space-between stretch':
                this.el.nativeElement.style.justifyContent = 'space-between';
                this.el.nativeElement.style.alignItems = 'stretch';
                break;
            case 'space-between center':
                this.el.nativeElement.style.justifyContent = 'space-between';
                this.el.nativeElement.style.alignItems = 'center';
                break;
            case 'space-between none':
                this.el.nativeElement.style.justifyContent = 'space-between';
                this.el.nativeElement.style.alignItems = 'none';
                break;
            case 'space-between baseline':
                this.el.nativeElement.style.justifyContent = 'space-between';
                this.el.nativeElement.style.alignItems = 'baseline';
                break;
            case 'center start':
                this.el.nativeElement.style.justifyContent = 'center';
                this.el.nativeElement.style.alignItems = 'flex-start';
                break;
            case 'space-around':
                this.el.nativeElement.style.justifyContent = 'space-around';
                this.el.nativeElement.style.alignItems = 'center';
                break;
            case 'end':
                this.el.nativeElement.style.justifyContent = 'flex-end';
                this.el.nativeElement.style.alignItems = 'center';
                break;
            case 'start stretch':
                this.el.nativeElement.style.justifyContent = 'flex-start';
                this.el.nativeElement.style.alignItems = 'stretch';
                break;
        }
    }
}
