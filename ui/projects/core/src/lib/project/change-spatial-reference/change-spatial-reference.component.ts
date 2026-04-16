import {Component, ChangeDetectionStrategy, ChangeDetectorRef, OnDestroy, inject} from '@angular/core';
import {ProjectService} from '../project.service';
import {SpatialReferenceService} from '../../spatial-references/spatial-reference.service';
import {Subscription} from 'rxjs/internal/Subscription';
import {NamedSpatialReference, SpatialReference} from '@geoengine/common';
import {SidenavHeaderComponent} from '../../sidenav/sidenav-header/sidenav-header.component';
import {MatFormField} from '@angular/material/input';
import {MatSelect} from '@angular/material/select';
import {FormsModule} from '@angular/forms';
import {MatOption} from '@angular/material/autocomplete';

@Component({
    selector: 'geoengine-change-projection',
    templateUrl: './change-spatial-reference.component.html',
    styleUrls: ['./change-spatial-reference.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [SidenavHeaderComponent, MatFormField, MatSelect, FormsModule, MatOption],
})
export class ChangeSpatialReferenceComponent implements OnDestroy {
    projectService = inject(ProjectService);
    protected spatialReferenceService = inject(SpatialReferenceService);
    protected changeDetectorRef = inject(ChangeDetectorRef);

    readonly SpatialReferences: Array<NamedSpatialReference>;

    spatialReference?: NamedSpatialReference;

    private subscription: Subscription;

    constructor() {
        this.SpatialReferences = this.spatialReferenceService.getSpatialReferences();
        this.subscription = this.projectService.getSpatialReferenceStream().subscribe((sref: SpatialReference) => {
            const index = this.SpatialReferences.findIndex((v) => v.spatialReference.srsString === sref.srsString);

            if (index >= 0) {
                this.spatialReference = this.SpatialReferences[index];
            } else {
                this.spatialReference = undefined;
            }

            this.changeDetectorRef.markForCheck();
        });
    }

    setSpatialReference(sref: NamedSpatialReference): void {
        this.projectService.setSpatialReference(new SpatialReference(sref.spatialReference.srsString));
    }

    ngOnDestroy(): void {
        this.subscription.unsubscribe();
    }
}
