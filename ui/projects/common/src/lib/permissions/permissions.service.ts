import {Injectable, inject} from '@angular/core';
import {Permission, PermissionListing, PermissionsApi} from '@geoengine/api-client';
import {ReplaySubject, firstValueFrom} from 'rxjs';
import {UserService, apiConfigurationWithAccessKey} from '../user/user.service';

export type ResourceType = 'dataset' | 'layer' | 'layerCollection' | 'project' | 'provider' | 'mlModel';

@Injectable({
    providedIn: 'root',
})
export class PermissionsService {
    private sessionService = inject(UserService);

    permissionsApi = new ReplaySubject<PermissionsApi>(1);

    constructor() {
        this.sessionService.getSessionStream().subscribe({
            next: (session) => this.permissionsApi.next(new PermissionsApi(apiConfigurationWithAccessKey(session.sessionToken))),
        });
    }

    async addPermission(resourceType: ResourceType, resourceId: string, roleId: string, permission: Permission): Promise<void> {
        const permissionsApi = await firstValueFrom(this.permissionsApi);

        return permissionsApi.addPermissionHandler({
            permissionRequest: {
                permission,
                resource: {id: resourceId, type: resourceType},
                roleId,
            },
        });
    }

    async getPermissions(resourceType: ResourceType, resourceId: string, offset = 0, limit = 20): Promise<PermissionListing[]> {
        const permissionsApi = await firstValueFrom(this.permissionsApi);

        return permissionsApi.getResourcePermissionsHandler({
            resourceType,
            resourceId,
            offset,
            limit,
        });
    }

    async removePermission(resourceType: ResourceType, resourceId: string, roleId: string, permission: Permission): Promise<void> {
        const permissionsApi = await firstValueFrom(this.permissionsApi);

        await permissionsApi.removePermissionHandler({
            permissionRequest: {
                permission,
                resource: {id: resourceId, type: resourceType},
                roleId,
            },
        });
    }
}
