import {CdkPortalOutlet, ComponentPortal} from '@angular/cdk/portal';
import {
    Component,
    ChangeDetectionStrategy,
    HostBinding,
    OnChanges,
    SimpleChanges,
    OnDestroy,
    ComponentFactoryResolver,
    Injector,
    ChangeDetectorRef,
    inject,
    input,
    viewChild,
} from '@angular/core';
import {Observable, Subscription} from 'rxjs';
import {map} from 'rxjs/operators';
import {CoreConfig} from '../config.service';
import {LayoutService} from '../layout.service';
import {clamp} from '../util/math';
import {TabContent, TabsService} from './tabs.service';
import {MatButton} from '@angular/material/button';
import {MatTooltip} from '@angular/material/tooltip';
import {MatIcon} from '@angular/material/icon';
import {MatTabNav, MatTabLink, MatTabNavPanel} from '@angular/material/tabs';
import {FxFlexDirective} from '@geoengine/common';
import {AsyncPipe} from '@angular/common';

const TAB_WIDTH_PCT_MIN = 10;
const TAB_WIDTH_PCT_MAX = 20;

@Component({
    selector: 'geoengine-tab-panel',
    templateUrl: './tabs.component.html',
    styleUrls: ['./tabs.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatButton, MatTooltip, MatIcon, MatTabNav, MatTabLink, FxFlexDirective, MatTabNavPanel, CdkPortalOutlet, AsyncPipe],
})
export class TabsComponent implements OnChanges, OnDestroy {
    readonly tabsService = inject(TabsService);
    protected readonly layoutService = inject(LayoutService);
    protected readonly config = inject(CoreConfig);
    protected readonly componentFactoryResolver = inject(ComponentFactoryResolver);
    protected readonly injector = inject(Injector);
    protected readonly changeDetectorRef = inject(ChangeDetectorRef);

    @HostBinding('class.mat-elevation-z4') elevationStyle = true;
    readonly portalOutlet = viewChild(CdkPortalOutlet);

    readonly maxHeight = input(0);
    readonly visible = input(true);

    toggleTooltip: 'Show' | 'Hide' = this.visible() ? 'Hide' : 'Show';
    readonly toggleTooltipDelay: number;

    readonly tabWidthPct: Observable<number>;

    contentHeight = 0;

    protected activeTabSubscription: Subscription;

    constructor() {
        this.toggleTooltipDelay = this.config.DELAYS.TOOLTIP;

        this.activeTabSubscription = this.tabsService.getActiveTabChanges().subscribe((tabContent) => {
            if (tabContent) {
                this.renderTabContent(tabContent);
            } else {
                this.removeRenderedTabContent();
            }
        });

        this.tabWidthPct = this.tabsService.tabs.pipe(map((tabs) => clamp(100 / tabs.length, TAB_WIDTH_PCT_MIN, TAB_WIDTH_PCT_MAX)));
    }

    ngOnChanges(changes: SimpleChanges): void {
        const visible = this.visible();
        if (changes.maxHeight || changes.visible) {
            this.setContentHeight(this.maxHeight(), visible);
        }

        if (changes.visible) {
            this.toggleTooltip = visible ? 'Hide' : 'Show';

            if (visible && this.tabsService.activeTab) {
                this.renderTabContent(this.tabsService.activeTab);
            } else if (!visible) {
                this.removeRenderedTabContent();
            }
        }
    }

    ngOnDestroy(): void {
        this.activeTabSubscription.unsubscribe();
    }

    toggleVisibility(): void {
        this.layoutService.toggleLayerDetailViewVisibility();
    }

    setTab(tabContent: TabContent): void {
        this.tabsService.activeTab = tabContent;
    }

    closeTab(tabContent: TabContent): void {
        this.tabsService.removeComponent(tabContent);
    }

    protected renderTabContent(tabContent: TabContent): void {
        this.layoutService.setLayerDetailViewVisibility(true);

        this.removeRenderedTabContent();

        const portal = new ComponentPortal(tabContent.component);
        const componentRef = this.portalOutlet()?.attach(portal);

        // inject data
        for (const property of Object.keys(tabContent.inputs)) {
            componentRef?.setInput(property, tabContent.inputs[property]);
        }

        this.changeDetectorRef.markForCheck();
    }

    protected removeRenderedTabContent(): void {
        const portalOutlet = this.portalOutlet();
        if (portalOutlet?.hasAttached()) {
            portalOutlet.detach();
        }
    }

    protected setContentHeight(maxHeight: number, visible: boolean): void {
        let contentHeight = 0;
        if (visible) {
            contentHeight = maxHeight - LayoutService.getLayerDetailViewBarHeightPx();
        }

        this.contentHeight = contentHeight;
    }
}
