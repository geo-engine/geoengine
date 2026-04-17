import {fromEvent, combineLatest, BehaviorSubject, Observable, ReplaySubject, Subject} from 'rxjs';
import {debounceTime, distinctUntilChanged, map} from 'rxjs/operators';
import {Injectable, Type, inject} from '@angular/core';
import {CoreConfig} from './config.service';
import {LayoutService as CommonLayoutService} from '@geoengine/common';

/**
 * Layout settings serialization format.
 */
export interface LayoutDict {
    layerListVisible: boolean;
    layerDetailViewVisible: boolean;
    layerDetailViewTabIndex: number;
    layerDetailViewHeightPercentage: number;
}

export interface SidenavConfig {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    component: Type<any>;
    keepParent?: boolean;
    parent?: SidenavConfig;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    config?: Record<string, any>;
}

/**
 * A service that keeps track of app layouting options.
 */
@Injectable()
export class LayoutService extends CommonLayoutService {
    protected config = inject(CoreConfig);

    private static _scrollbarWidthPx: number;

    /**
     * Is the layer list visible?
     */
    private layerListVisible$: BehaviorSubject<boolean> = new BehaviorSubject<boolean>(true);

    /**
     * Is the data table visible?
     */
    private layerDetailViewVisible$: BehaviorSubject<boolean> = new BehaviorSubject<boolean>(true);

    /**
     * What is the currently visible tab?
     */
    private layerDetailViewTabIndex$ = new BehaviorSubject<number>(0);

    /**
     * What is the height of the layer detail view as a percentage of the available space.
     */
    private layerDetailViewHeightPercentage$ = new BehaviorSubject<number>(2 / 5);

    /**
     *  Sidenav content
     */
    private sidenavContentComponent$: Subject<SidenavConfig | undefined> = new ReplaySubject(1);

    private sidenavContentMaxWidth$: Subject<number> = new ReplaySubject(1);

    constructor() {
        super();
        this.setupSidenavWidthStream();
    }

    static scrollbarWidthPx(): number {
        if (!this._scrollbarWidthPx) {
            const outer = document.createElement('div');
            outer.style.visibility = 'hidden';
            outer.style.width = '100px';

            document.body.appendChild(outer);

            const widthNoScroll = outer.offsetWidth;
            // force scrollbars
            outer.style.overflow = 'scroll';

            // add innerdiv
            const inner = document.createElement('div');
            inner.style.width = '100%';
            outer.appendChild(inner);

            const widthWithScroll = inner.offsetWidth;

            // remove divs
            if (outer.parentNode) {
                outer.parentNode.removeChild(outer);
            }

            this._scrollbarWidthPx = widthNoScroll - widthWithScroll;
        }

        return this._scrollbarWidthPx;
    }

    static getToolbarHeightPx(): number {
        const mobileLandscape = window.matchMedia('(max-width: 960px) and (orientation: landscape)').matches;
        if (mobileLandscape) {
            return 48;
        }

        const mobilePortrait = window.matchMedia('(max-width: 600px) and (orientation: portrait)').matches;
        if (mobilePortrait) {
            return 56;
        }

        return 64;
    }

    static getLayerDetailViewBarHeightPx(): number {
        const mobileLandscape = window.matchMedia('(max-width: 960px) and (orientation: landscape)').matches;
        if (mobileLandscape) {
            return 2 * LayoutService.remInPx;
        }

        const mobilePortrait = window.matchMedia('(max-width: 600px) and (orientation: portrait)').matches;
        if (mobilePortrait) {
            return 2.5 * LayoutService.remInPx;
        }

        const borderSizePx = 1;

        return 3 * LayoutService.remInPx + borderSizePx;
    }

    getSidenavWidthStream(): Observable<number> {
        return this.sidenavContentMaxWidth$;
    }

    /**
     * Which component to show in the sidenav?
     */
    getSidenavContentComponentStream(): Observable<SidenavConfig | undefined> {
        return this.sidenavContentComponent$.pipe(distinctUntilChanged());
    }

    /**
     * Set the new Component to show in the sidenav
     */
    setSidenavContentComponent(sidenavConfig: SidenavConfig | undefined): void {
        this.sidenavContentComponent$.next(sidenavConfig);
    }

    /**
     * Is the layer list visible in the component?
     */
    getLayerListVisibility(): boolean {
        return this.layerListVisible$.value;
    }

    /**
     * Is the layer list visible in the component?
     */
    getLayerListVisibilityStream(): Observable<boolean> {
        return this.layerListVisible$;
    }

    /**
     * Sets the visibility of the layer list.
     */
    setLayerListVisibility(visible: boolean): void {
        this.layerListVisible$.next(visible);
    }

    /**
     * Toggles the visibility of the layer list.
     */
    toggleLayerListVisibility(): void {
        this.setLayerListVisibility(!this.layerListVisible$.value);
    }

    /**
     * Is the layer detail view visible?
     */
    getLayerDetailViewVisibilityStream(): Observable<boolean> {
        return this.layerDetailViewVisible$;
    }

    /**
     * Is the layer detail view visible?
     */
    getLayerDetailViewVisibility(): boolean {
        return this.layerDetailViewVisible$.value;
    }

    /**
     * Sets the visibility of the layer detail view.
     */
    setLayerDetailViewVisibility(visible: boolean): void {
        this.layerDetailViewVisible$.next(visible);
    }

    /**
     * Toggles the visibility of the layer detail view.
     */
    toggleLayerDetailViewVisibility(): void {
        this.setLayerDetailViewVisibility(!this.layerDetailViewVisible$.value);
    }

    /**
     * What is the current tab index?
     */
    getLayerDetailViewTabIndexStream(): Observable<number> {
        return this.layerDetailViewTabIndex$;
    }

    /**
     * What is the current tab index?
     */
    getLayerDetailViewTabIndex(): number {
        return this.layerDetailViewTabIndex$.value;
    }

    /**
     * Set the current tab index.
     */
    setLayerDetailViewTabIndex(index: number): void {
        // ignore call if it is the same index.
        if (this.layerDetailViewTabIndex$.value === index) {
            return;
        }

        if (index < 0) {
            index = 0; // repair index
        }

        this.layerDetailViewTabIndex$.next(index);
    }

    /**
     * Sets the percentage of the vertical viewport that the data table covers.
     */
    setLayerDetailViewHeightPercentage(percentage: number): void {
        if (percentage < 0 || percentage > 1) {
            throw Error('The data table percentage value must be between 0 and 1.');
        }

        this.layerDetailViewHeightPercentage$.next(percentage);
    }

    /**
     * Calculate the height of the data table.
     */
    getLayerDetailViewHeight(totalAvailableHeight: number): number {
        return LayoutService.calculateLayerDetailViewHeight(
            this.layerDetailViewVisible$.value ? this.layerDetailViewHeightPercentage$.value : 0,
            totalAvailableHeight,
        );
    }

    /**
     * Calculate the height of the data table.
     */
    getLayerDetailViewStream(totalAvailableHeight$: Observable<number>): Observable<number> {
        return combineLatest([this.layerDetailViewHeightPercentage$, totalAvailableHeight$, this.layerDetailViewVisible$]).pipe(
            map(([layerDetailViewHeightPercentage, totalAvailableHeight, layerDetailViewVisible]): number =>
                LayoutService.calculateLayerDetailViewHeight(
                    layerDetailViewVisible ? layerDetailViewHeightPercentage : 0,
                    totalAvailableHeight,
                ),
            ),
        );
    }

    /**
     * Calculate the height of the map.
     */
    getMapHeight(totalAvailableHeight: number): number {
        return LayoutService.calculateMapHeight(
            this.layerDetailViewVisible$.value ? this.layerDetailViewHeightPercentage$.value : 0,
            totalAvailableHeight,
        );
    }

    /**
     * Calculate the height of the data table.
     */
    getMapHeightStream(totalAvailableHeight$: Observable<number>): Observable<number> {
        return combineLatest([this.layerDetailViewHeightPercentage$, totalAvailableHeight$, this.layerDetailViewVisible$]).pipe(
            map(([layerDetailViewHeightPercentage, totalAvailableHeight, layerDetailViewVisible]): number =>
                LayoutService.calculateMapHeight(layerDetailViewVisible ? layerDetailViewHeightPercentage : 0, totalAvailableHeight),
            ),
        );
    }

    getLayoutDict(): LayoutDict {
        return {
            layerListVisible: this.getLayerListVisibility(),
            layerDetailViewVisible: this.getLayerDetailViewVisibility(),
            layerDetailViewTabIndex: this.getLayerDetailViewTabIndex(),
            layerDetailViewHeightPercentage: this.layerDetailViewHeightPercentage$.getValue(),
        };
    }

    getLayoutDictStream(): Observable<LayoutDict> {
        return combineLatest([
            this.layerListVisible$,
            this.layerDetailViewVisible$,
            this.layerDetailViewTabIndex$,
            this.layerDetailViewHeightPercentage$,
        ]).pipe(
            map(([layerListVisible, layerDetailViewVisible, layerDetailViewTabIndex, layerDetailViewHeightPercentage]) => ({
                layerListVisible,
                layerDetailViewVisible,
                layerDetailViewTabIndex,
                layerDetailViewHeightPercentage,
            })),
        );
    }

    setLayoutDict(dict: LayoutDict): void {
        if (typeof dict.layerListVisible === 'boolean') {
            this.setLayerListVisibility(dict.layerListVisible);
        }
        if (typeof dict.layerDetailViewVisible === 'boolean') {
            this.setLayerDetailViewVisibility(dict.layerDetailViewVisible);
        }
        if (typeof dict.layerDetailViewTabIndex === 'number') {
            this.setLayerDetailViewTabIndex(dict.layerDetailViewTabIndex);
        }
        if (typeof dict.layerDetailViewHeightPercentage === 'number') {
            this.setLayerDetailViewHeightPercentage(dict.layerDetailViewHeightPercentage);
        }
    }

    /**
     * Calculate the height of the data table.
     */
    private static calculateLayerDetailViewHeight(layerDetailViewHeightPercentage: number, totalAvailableHeight: number): number {
        return Math.max(Math.ceil(layerDetailViewHeightPercentage * totalAvailableHeight), LayoutService.getLayerDetailViewBarHeightPx());
    }

    /**
     * Calculate the height of the map.
     */
    private static calculateMapHeight(layerDetailViewHeightPercentage: number, totalAvailableHeight: number): number {
        const layerDetailViewHeight = LayoutService.calculateLayerDetailViewHeight(layerDetailViewHeightPercentage, totalAvailableHeight);
        return totalAvailableHeight - layerDetailViewHeight;
    }

    /**
     * Initialize and update the sidenav INNER width stream
     */
    private setupSidenavWidthStream(): void {
        const getWidth = (): number => {
            const sidenavComponent = document.getElementsByTagName('mat-sidenav')[0];
            const sidenavStyle = window.getComputedStyle(sidenavComponent);
            const widthString = sidenavStyle.width;

            if (widthString === 'auto') {
                return 0;
            } else if (widthString.endsWith('px')) {
                return parseFloat(widthString.substr(0, widthString.length - 2)) - 4 * LayoutService.remInPx;
            } else {
                throw new Error('sidenav width must be a `px` value');
            }
        };

        // this timeout prevents calling the `getWidth` function before the DOM is initialized
        setTimeout(() => {
            fromEvent(window, 'resize')
                .pipe(debounceTime(this.config.DELAYS.DEBOUNCE))
                .subscribe(() => {
                    this.sidenavContentMaxWidth$.next(getWidth());
                });

            this.sidenavContentMaxWidth$.next(getWidth());
        });
    }
}
