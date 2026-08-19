import {Component, viewChild, ChangeDetectionStrategy} from '@angular/core';
import {DatasetListing} from '@geoengine/api-client';
import {BehaviorSubject} from 'rxjs';
import {DatasetListComponent} from './dataset-list/dataset-list.component';
import {DatasetEditorComponent} from './dataset-editor/dataset-editor.component';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-manager-datasets',
    templateUrl: './datasets.component.html',
    styleUrl: './datasets.component.scss',
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [DatasetListComponent, DatasetEditorComponent, AsyncPipe],
})
export class DatasetsComponent {
    readonly datasetList = viewChild.required(DatasetListComponent);

    selectedDataset$: BehaviorSubject<DatasetListing | undefined> = new BehaviorSubject<DatasetListing | undefined>(undefined);

    selectDataset(dataset: DatasetListing | undefined): void {
        this.selectedDataset$.next(dataset);
    }

    datasetDeleted(): void {
        this.datasetList().backToAllDatasets();
        this.selectedDataset$.next(undefined);
    }
}
