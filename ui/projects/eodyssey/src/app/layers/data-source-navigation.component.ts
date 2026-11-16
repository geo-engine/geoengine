import {ChangeDetectionStrategy, Component, inject} from '@angular/core';
import {MatExpansionModule} from '@angular/material/expansion';
import {MatListModule} from '@angular/material/list';
import {MatTooltipModule} from '@angular/material/tooltip';
import {EOdysseyLayerService} from './layers.service';

@Component({
    selector: 'geoengine-data-source-navigation',
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatExpansionModule, MatListModule, MatTooltipModule],
    template: `
        <mat-accordion displayMode="flat" [multi]="false" [attr.aria-busy]="layers.catalogueLoading()">
            @for (category of layers.categories(); track category.key) {
                <mat-expansion-panel
                    [expanded]="layers.selectedCategory()?.key === category.key"
                    (opened)="layers.setSelectedCategory(category.key)"
                >
                    <mat-expansion-panel-header>
                        <mat-panel-title>{{ category.name }}</mat-panel-title>
                    </mat-expansion-panel-header>
                    <mat-selection-list
                        color="primary"
                        [multiple]="false"
                        [attr.aria-label]="category.name + ' data sources'"
                        (selectionChange)="selectSource($event.options)"
                    >
                        @for (source of category.sources; track source.key) {
                            <mat-list-option
                                [value]="source.key"
                                [selected]="layers.selectedDataSource()?.key === source.key"
                                [matTooltip]="source.name"
                            >
                                <span matListItemTitle>{{ source.name }}</span>
                            </mat-list-option>
                        }
                    </mat-selection-list>
                    @if (!category.sources.length) {
                        <p role="status">No data sources are available in this category.</p>
                    }
                </mat-expansion-panel>
            }
        </mat-accordion>
    `,
    styles: `
        :host {
            display: block;
            min-width: 0;
        }

        mat-accordion {
            display: block;
            overflow: hidden;
            border: 1px solid var(--geoengine-divider-color);
            border-radius: 0.5rem;

            --mat-expansion-container-shape: 0;
            --mat-expansion-container-elevation-shadow: none;
            --mat-expansion-header-collapsed-state-height: 48px;
            --mat-expansion-header-expanded-state-height: 48px;
            --mat-expansion-header-hover-state-layer-color: color-mix(
                in srgb,
                var(--geoengine-primary-color) 8%,
                var(--geoengine-card-background-color)
            );
            --mat-expansion-header-focus-state-layer-color: color-mix(
                in srgb,
                var(--geoengine-primary-color) 10%,
                var(--geoengine-card-background-color)
            );
        }

        mat-expansion-panel + mat-expansion-panel {
            border-top: 1px solid var(--geoengine-divider-color);
        }

        mat-expansion-panel-header {
            padding: 0 0.75rem;
            background: color-mix(in srgb, var(--geoengine-primary-color) 4%, var(--geoengine-card-background-color));

            &.mat-expanded {
                border-bottom: 1px solid var(--geoengine-divider-color);
                box-shadow: inset 3px 0 var(--geoengine-primary-color);

                --mat-expansion-header-text-color: var(--geoengine-primary-color);
                --mat-expansion-header-indicator-color: var(--geoengine-primary-color);
            }

            &.mat-expanded,
            &.mat-expanded:hover,
            &.mat-expanded:focus {
                background: color-mix(in srgb, var(--geoengine-primary-color) 12%, var(--geoengine-card-background-color));
            }

            &:focus-visible {
                outline: 2px solid var(--geoengine-primary-color);
                outline-offset: -3px;
            }
        }

        mat-panel-title {
            font-size: 0.85rem;
            font-weight: 600;
        }

        mat-selection-list {
            padding: 0;
        }

        mat-list-option {
            height: auto;
            min-height: 3rem;
            border-radius: 0.25rem;
            padding: 0.5rem;
            --mat-list-list-item-label-text-size: 0.85rem;
            --mat-list-list-item-label-text-line-height: 1.5;

            &[aria-selected='true'] {
                background: color-mix(in srgb, var(--geoengine-primary-color) 7%, var(--geoengine-card-background-color));
            }

            &:focus-visible {
                outline: 2px solid var(--geoengine-primary-color);
                outline-offset: -2px;
            }

            span {
                display: -webkit-box !important;
                -webkit-line-clamp: 2;
                -webkit-box-orient: vertical;
                overflow: hidden;
                white-space: normal !important;
            }
        }

        p {
            margin: 0;
            padding: 0.5rem;
            font-size: 0.85rem;
            color: var(--geoengine-foreground-secondary-text-color);
        }

        :host ::ng-deep .mat-expansion-panel-body {
            padding: 0.25rem;
        }

        :host ::ng-deep mat-list-option .mdc-list-item__end {
            margin-inline-start: 0.5rem;
            margin-inline-end: 0.25rem;
        }

        :host ::ng-deep mat-list-option .mdc-radio {
            padding: 0;
        }
    `,
})
export class DataSourceNavigationComponent {
    readonly layers = inject(EOdysseyLayerService);

    selectSource(options: readonly {value: string}[]): void {
        const key = options[0]?.value;
        if (key) this.layers.setSelectedDataSource(key);
    }
}
