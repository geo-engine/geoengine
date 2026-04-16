import {Component, ChangeDetectionStrategy, inject, input} from '@angular/core';
import {concatMap, first, from, Observable, range, reduce, takeWhile} from 'rxjs';
import {LayoutService, SidenavConfig} from '../../layout.service';
import {AddWorkflowComponent} from '../add-workflow/add-workflow.component';
import {DrawFeaturesComponent} from '../draw-features/draw-features.component';
import {UploadComponent} from '../upload/upload.component';
import {LayersService} from '@geoengine/common';
import {LayerCollectionSelectionComponent} from '../../layer-collections/layer-collection-selection.component';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {MatNavList, MatListItem, MatListItemIcon, MatListItemTitle, MatListItemLine} from '@angular/material/list';
import {MatIcon} from '@angular/material/icon';

export interface AddDataButton {
    name: string;
    description: string;
    icon?: string;
    iconSrc?: string;
    sidenavConfig: SidenavConfig | undefined;
    // TODO: restrict registered/anonymous? Tie to role/groups?
}

@Component({
    selector: 'geoengine-add-data',
    templateUrl: './add-data.component.html',
    styleUrls: ['./add-data.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [SidenavHeaderComponent, MatNavList, MatListItem, MatListItemIcon, MatIcon, MatListItemTitle, MatListItemLine],
})
export class AddDataComponent {
    protected readonly layoutService = inject(LayoutService);

    /**
     * A list of data source dialogs to display
     */
    readonly buttons = input.required<Array<AddDataButton>>();

    /**
     * Load a selected component into the sidenav
     */
    setComponent(sidenavConfig: SidenavConfig | undefined): void {
        if (!sidenavConfig) {
            return;
        }

        this.layoutService.setSidenavContentComponent(sidenavConfig);
    }

    static createLayerRootCollectionButtons(layersService: LayersService): Observable<Array<AddDataButton>> {
        const MAX_NUMBER_OF_QUERIES = 10;
        const BATCH_SIZE = 20;
        return range(0, MAX_NUMBER_OF_QUERIES).pipe(
            concatMap((i) => {
                const start = i * BATCH_SIZE;
                return from(layersService.getRootLayerCollectionItems(start, BATCH_SIZE)).pipe(first());
            }),
            takeWhile((collection) => collection.items.length > 0),
            reduce((acc, collection) => {
                const buttons: Array<AddDataButton> = collection.items.map((item) => ({
                    name: item.name,
                    description: item.description,
                    icon: 'layers',
                    sidenavConfig: {
                        component: LayerCollectionSelectionComponent,
                        keepParent: true,
                        config: {collectionId: item.id, collectionName: item.name},
                    },
                }));
                return acc.concat(buttons);
            }, [] as Array<AddDataButton>),
        );
    }

    static createUploadButton(): AddDataButton {
        return {
            name: 'Upload',
            description: 'Upload data from your local computer',
            icon: 'publish',
            sidenavConfig: {component: UploadComponent, keepParent: true},
        };
    }

    /**
     * Default for a draw features entry.
     */
    static createDrawFeaturesButton(): AddDataButton {
        return {
            name: 'Draw Features',
            description: 'Draw features on the map',
            icon: 'create',
            sidenavConfig: {component: DrawFeaturesComponent, keepParent: true},
        };
    }

    /**
     * Add workflow id dialog
     */
    static createAddWorkflowByIdButton(): AddDataButton {
        return {
            name: 'Add Workflow by Id',
            description: 'Add a workflow by its id',
            icon: 'build',
            sidenavConfig: {component: AddWorkflowComponent, keepParent: true},
        };
    }
}
