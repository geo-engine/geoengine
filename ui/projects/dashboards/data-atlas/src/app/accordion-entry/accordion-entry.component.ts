import {Component, OnInit, ChangeDetectionStrategy, ChangeDetectorRef, inject, input} from '@angular/core';
import {mergeMap, BehaviorSubject, combineLatest, of, forkJoin, Observable, map, from} from 'rxjs';
import {LayerCollectionListingDict, ProjectService, ProviderLayerCollectionIdDict} from '@geoengine/core';
import {DataRange, DataSelectionService} from '../data-selection.service';
import {LayersService, RasterLayer, RasterSymbology, Time, CommonModule, AsyncValueDefault} from '@geoengine/common';
import {CollectionItem, LayerCollection, LayerListing, ProviderLayerId} from '@geoengine/api-client';
import {MatCard, MatCardHeader, MatCardAvatar, MatCardTitle, MatCardSubtitle, MatCardContent} from '@angular/material/card';
import {MatIcon} from '@angular/material/icon';
import {MatButton} from '@angular/material/button';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-accordion-entry',
    templateUrl: './accordion-entry.component.html',
    styleUrls: ['./accordion-entry.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [
        MatCard,
        MatCardHeader,
        MatIcon,
        MatCardAvatar,
        MatCardTitle,
        MatCardSubtitle,
        MatCardContent,
        CommonModule,
        MatButton,
        AsyncPipe,
        AsyncValueDefault,
    ],
})
export class AccordionEntryComponent implements OnInit {
    private readonly layersService = inject(LayersService);
    readonly projectService = inject(ProjectService);
    readonly dataSelectionService = inject(DataSelectionService);
    private readonly changeDetectorRef = inject(ChangeDetectorRef);

    readonly collection = input.required<ProviderLayerCollectionIdDict>();
    readonly otherCollection = input<ProviderLayerCollectionIdDict>();
    readonly icon = input('class');

    readonly selectedLayers$ = new BehaviorSubject<Array<ProviderLayerId | undefined>>([]);
    readonly collections$ = new BehaviorSubject<Array<LayerCollection>>([]);

    ngOnInit(): void {
        let otherCollectionItems$: Observable<Array<CollectionItem>> = of([]);
        const otherCollection = this.otherCollection();
        if (otherCollection) {
            otherCollectionItems$ = from(
                this.layersService.getLayerCollectionItems(otherCollection.providerId, otherCollection.collectionId),
            ).pipe(map((c) => c.items));
        }

        forkJoin({
            providerCollections: this.layersService.getLayerCollectionItems(this.collection().providerId, this.collection().collectionId),
            otherCollectionItems: otherCollectionItems$,
        }).subscribe(({providerCollections, otherCollectionItems}) => {
            const collections = [];

            for (const item of providerCollections.items) {
                if (item.type === 'collection') {
                    const collection = item as LayerCollectionListingDict;
                    collections.push(this.layersService.getLayerCollectionItems(collection.id.providerId, collection.id.collectionId));
                }
            }

            for (const item of otherCollectionItems) {
                if (item.type === 'collection') {
                    const collection = item as LayerCollectionListingDict;
                    collections.push(this.layersService.getLayerCollectionItems(collection.id.providerId, collection.id.collectionId));
                }
            }

            combineLatest(collections).subscribe((col) => {
                col.sort((a, b) => a.name.localeCompare(b.name));

                this.collections$.next(col);
                this.selectedLayers$.next(new Array(col.length).fill(undefined));
            });
        });
    }

    layerSelected(i: number, layer: LayerListing | undefined): void {
        const selected = this.selectedLayers$.getValue();
        selected[i] = layer?.id;
        this.selectedLayers$.next(selected);
    }

    loadData(i: number): void {
        const id = this.selectedLayers$.getValue()[i];
        if (!id) {
            return;
        }

        from(this.layersService.getLayer(id.providerId, id.layerId))
            .pipe(
                mergeMap((layer) => combineLatest([of(layer), this.projectService.registerWorkflow(layer.workflow)])),
                mergeMap(([layer, workflowId]) => {
                    if (!layer.symbology) {
                        throw new Error('Layer has no symbology');
                    }

                    if (!layer.metadata) {
                        throw new Error('Layer has no metadata');
                    }

                    if (!('timeSteps' in layer.metadata)) {
                        throw new Error('Layer has no timeSteps');
                    }

                    if (!('dataRange' in layer.metadata)) {
                        throw new Error('Layer has no dataRange');
                    }

                    const timeSteps: Array<Time> = JSON.parse(layer.metadata['timeSteps']).map((t: number) => new Time(t));

                    const range: [number, number] = JSON.parse(layer.metadata['dataRange']);
                    const dataRange: DataRange = {
                        min: range[0],
                        max: range[1],
                    };

                    const rasterLayer = new RasterLayer({
                        name: 'EBV',
                        workflowId,
                        isVisible: true,
                        isLegendVisible: false,
                        symbology: RasterSymbology.fromDict(layer.symbology) as RasterSymbology,
                    });

                    return this.dataSelectionService.setRasterLayer(rasterLayer, timeSteps, dataRange);
                }),
            )
            .subscribe(() => this.changeDetectorRef.markForCheck());
    }
}
