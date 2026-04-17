import {Injectable, inject} from '@angular/core';
import {TypedResultDescriptor, Workflow, WorkflowsApi} from '@geoengine/api-client';
import {ReplaySubject, firstValueFrom} from 'rxjs';
import {UserService, apiConfigurationWithAccessKey} from '../user/user.service';
import {UUID} from '../datasets/dataset.model';

@Injectable({
    providedIn: 'root',
})
export class WorkflowsService {
    private sessionService = inject(UserService);

    workflowApi = new ReplaySubject<WorkflowsApi>(1);

    constructor() {
        this.sessionService.getSessionStream().subscribe({
            next: (session) => this.workflowApi.next(new WorkflowsApi(apiConfigurationWithAccessKey(session.sessionToken))),
        });
    }

    async getWorkflow(id: string): Promise<Workflow> {
        const workflowApi = await firstValueFrom(this.workflowApi);

        return workflowApi.loadWorkflowHandler({
            id,
        });
    }

    async getMetadata(id: string): Promise<TypedResultDescriptor> {
        const workflowApi = await firstValueFrom(this.workflowApi);

        return workflowApi.getWorkflowMetadataHandler({
            id,
        });
    }

    async registerWorkflow(workflow: Workflow): Promise<UUID> {
        const workflowApi = await firstValueFrom(this.workflowApi);

        return workflowApi
            .registerWorkflowHandler({
                workflow,
            })
            .then((response) => response.id);
    }
}
