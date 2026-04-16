import {AfterViewInit, ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, inject, viewChild} from '@angular/core';
import {BehaviorSubject, Subscription} from 'rxjs';
import {MatPaginator} from '@angular/material/paginator';
import {MatTableDataSource, MatTable, MatColumnDef, MatCellDef, MatCell, MatRowDef, MatRow} from '@angular/material/table';
import {UserService} from '@geoengine/common';
import {AsyncPipe} from '@angular/common';

@Component({
    selector: 'geoengine-roles',
    templateUrl: './roles.component.html',
    styleUrls: ['./roles.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatTable, MatColumnDef, MatCellDef, MatCell, MatRowDef, MatRow, MatPaginator, AsyncPipe],
})
export class RolesComponent implements AfterViewInit, OnDestroy {
    protected readonly userService = inject(UserService);
    protected readonly changeDetectorRef = inject(ChangeDetectorRef);

    roleNames: Array<string> | undefined;
    displayedColumns: string[] = ['roleName'];
    roleTable: MatTableDataSource<string> | undefined;
    numberOfRoles = new BehaviorSubject<number>(0);

    readonly paginator = viewChild.required(MatPaginator);

    private roleNamesSubscription: Subscription | undefined;

    ngAfterViewInit(): void {
        this.roleNamesSubscription = this.userService.getRoleDescriptions().subscribe((roleNames) => {
            if (roleNames) {
                this.roleNames = roleNames.filter((x) => !x.individual).map((x) => x.role.name);
                this.numberOfRoles.next(this.roleNames.length);
                this.roleTable = new MatTableDataSource(this.roleNames);
                this.roleTable.paginator = this.paginator();
            } else {
                this.roleNames = undefined;
                this.numberOfRoles.next(0);
                this.roleTable = undefined;
            }
            this.changeDetectorRef.detectChanges();
        });
    }

    ngOnDestroy(): void {
        this.roleNamesSubscription?.unsubscribe();
    }
}
