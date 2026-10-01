import {vi, type Mock} from 'vitest';
import {Project} from './project.model';
import {ProjectService} from './project.service';
import moment from 'moment';
import {Session} from '../users/session.model';
import {User} from '../users/user.model';
import {firstValueFrom, NEVER, of, BehaviorSubject} from 'rxjs';
import {CoreConfig, DEFAULT_CORE_CONFIG} from '../config.service';
import {CreateProjectResponseDict, TimeStepDict, UUID} from '../backend/backend.model';
import {MapService, ViewportSize, Extent} from '../map/map.service';
import {BackendService} from '../backend/backend.service';
import {SpatialReferenceService, WGS_84} from '../spatial-references/spatial-reference.service';
import {first, mergeMap, tap} from 'rxjs/operators';
import {Configuration, DefaultConfig} from '@geoengine/api-client';
import {
    LayersService,
    NotificationService,
    RasterLayer,
    RasterSymbology,
    SpatialReferenceSpecification,
    Time,
    UserService,
} from '@geoengine/common';
import {TestBed} from '@angular/core/testing';

describe('test project methods in projectService', () => {
    let notificationServiceSpy: {
        error: Mock;
    };
    let mapServiceSpy: {
        getViewportSizeStream: Mock;
    };
    let backendSpy: {
        createProject: Mock;
        listProjects: Mock;
        setSessionProject: Mock;
        updateProject: Mock;
        getWorkflowMetadata: Mock;
    };
    let userServiceSpy: {
        getSessionStream: Mock;
        getSessionTokenStream: Mock;
        getSessionTokenForRequest: Mock;
    };
    let spatialReferenceSpy: {
        getSpatialReferenceSpecification: Mock;
    };
    let layersServiceSpy: {
        resolveLayer: Mock;
    };

    let projectService: ProjectService;

    beforeEach(() => {
        notificationServiceSpy = {
            error: vi.fn().mockName('NotificationService.error'),
        };
        mapServiceSpy = {
            getViewportSizeStream: vi.fn().mockName('MapService.getViewportSizeStream'),
        };
        backendSpy = {
            createProject: vi.fn().mockName('BackendService.createProject'),
            listProjects: vi.fn().mockName('BackendService.listProjects'),
            setSessionProject: vi.fn().mockName('BackendService.setSessionProject'),
            updateProject: vi.fn().mockName('BackendService.updateProject'),
            getWorkflowMetadata: vi.fn().mockName('BackendService.getWorkflowMetadata'),
        };
        userServiceSpy = {
            getSessionStream: vi.fn().mockName('UserService.getSessionStream'),
            getSessionTokenStream: vi.fn().mockName('UserService.getSessionTokenStream'),
            getSessionTokenForRequest: vi.fn().mockName('UserService.getSessionTokenForRequest'),
        };
        spatialReferenceSpy = {
            getSpatialReferenceSpecification: vi.fn().mockName('SpatialRefernceService.getSpatialReferenceSpecification'),
        };
        layersServiceSpy = {
            resolveLayer: vi.fn().mockName('LayersSerivce.resolveLayer'),
        };

        const sessionToken = 'ffffffff-ffff-4fff-afff-ffffffffffff';

        // always return the same session
        userServiceSpy.getSessionStream.mockReturnValue(
            of<Session>({
                sessionToken,
                apiConfiguration: new Configuration({
                    basePath: DefaultConfig.basePath,
                    fetchApi: DefaultConfig.fetchApi,
                    middleware: DefaultConfig.middleware,
                    queryParamsStringify: DefaultConfig.queryParamsStringify,
                    username: DefaultConfig.username,
                    password: DefaultConfig.password,
                    apiKey: DefaultConfig.apiKey,
                    accessToken: sessionToken,
                    headers: DefaultConfig.headers,
                    credentials: DefaultConfig.credentials,
                }),
                user: new User({
                    id: 'cccccccc-cccc-4ccc-accc-cccccccccccc',
                }),
                validUntil: moment.utc('3000-01-01'),
            }),
        );

        userServiceSpy.getSessionTokenForRequest.mockReturnValue(of<UUID>(sessionToken));
        userServiceSpy.getSessionTokenStream.mockReturnValue(of<UUID>(sessionToken));

        spatialReferenceSpy.getSpatialReferenceSpecification.mockReturnValue(
            of<SpatialReferenceSpecification>(
                new SpatialReferenceSpecification({
                    name: 'WGS84',
                    spatialReference: 'EPSG:4326',
                    projString: '+proj=longlat +datum=WGS84 +no_defs +type=crs',
                    extent: {
                        lowerLeftCoordinate: {
                            x: -180,
                            y: -90,
                        },
                        upperRightCoordinate: {
                            x: 180,
                            y: 90,
                        },
                    },
                    axisLabels: ['longitude', 'latitude'],
                }),
            ),
        );

        // for constructor
        backendSpy.listProjects.mockReturnValue(NEVER); // never complete and set any project

        backendSpy.getWorkflowMetadata.mockReturnValue(NEVER); // layer metadata is not of interest here

        backendSpy.createProject.mockReturnValue(
            of<CreateProjectResponseDict>({
                id: 'dddddddd-dddd-4ddd-addd-dddddddddddd',
            }),
        );

        backendSpy.updateProject.mockReturnValue(
            of<CreateProjectResponseDict>({
                id: 'dddddddd-dddd-4ddd-addd-dddddddddddd',
            }),
        );

        TestBed.configureTestingModule({
            providers: [
                ProjectService,
                {provide: CoreConfig, useValue: DEFAULT_CORE_CONFIG},
                {provide: NotificationService, useValue: notificationServiceSpy},
                {provide: MapService, useValue: mapServiceSpy},
                {provide: BackendService, useValue: backendSpy},
                {provide: UserService, useValue: userServiceSpy},
                {provide: SpatialReferenceService, useValue: spatialReferenceSpy},
                {provide: LayersService, useValue: layersServiceSpy},
            ],
        });

        projectService = TestBed.inject(ProjectService);
    });

    it('#createDefaultProject should create a default project', async () => {
        const project = await firstValueFrom(projectService.createDefaultProject());
        await expect(project.toDict()).toEqual(
            new Project({
                id: 'dddddddd-dddd-4ddd-addd-dddddddddddd',
                name: 'Default',
                description: 'Default project',
                spatialReference: WGS_84.spatialReference,
                time: new Time(moment.utc('2014-04-01 12:00:00')),
                bbox: {lowerLeftCoordinate: {x: -180, y: -90}, upperRightCoordinate: {x: 180, y: 90}},
                plots: [],
                layers: [],
                timeStepDuration: {
                    durationAmount: 1,
                    durationUnit: 'month',
                },
                version: {
                    changed: project.version.changed,
                    id: '0',
                },
            }).toDict(),
        );
        await expect(backendSpy.createProject).toHaveBeenCalledTimes(1);
        expect(backendSpy.createProject).toHaveBeenCalledWith(
            {
                name: 'Default',
                description: 'Default project',
                bounds: {
                    boundingBox: {
                        lowerLeftCoordinate: {
                            x: -180,
                            y: -90,
                        },
                        upperRightCoordinate: {
                            x: 180,
                            y: 90,
                        },
                    },
                    spatialReference: 'EPSG:4326',
                    timeInterval: {start: 1396353600000, end: 1396353600000},
                },
                timeStep: {
                    step: 1,
                    granularity: 'months',
                } as TimeStepDict,
            },
            'ffffffff-ffff-4fff-afff-ffffffffffff',
        );
    });

    it('#createProject should create a project', async () => {
        const project = await firstValueFrom(
            projectService.createProject({
                name: 'testProject',
                description: 'testDescription',
                spatialReference: WGS_84.spatialReference,
                time: new Time(moment.utc('2021-07-04 11:00:00')),
                bounds: {lowerLeftCoordinate: {x: -180, y: -90}, upperRightCoordinate: {x: 180, y: 90}},
                timeStepDuration: {
                    durationAmount: 1,
                    durationUnit: 'month',
                },
            }),
        );
        await expect(project.toDict()).toEqual(
            new Project({
                id: 'dddddddd-dddd-4ddd-addd-dddddddddddd',
                name: 'testProject',
                description: 'testDescription',
                spatialReference: WGS_84.spatialReference,
                time: new Time(moment.utc('2021-07-04 11:00:00')),
                bbox: {lowerLeftCoordinate: {x: -180, y: -90}, upperRightCoordinate: {x: 180, y: 90}},
                plots: [],
                layers: [],
                timeStepDuration: {
                    durationAmount: 1,
                    durationUnit: 'month',
                },
                version: {
                    changed: project.version.changed,
                    id: '0',
                },
            }).toDict(),
        );
    });

    it('#cloneProject should clone a project', async () => {
        // create project instance
        projectService.createDefaultProject().subscribe((project) => {
            projectService.setProject(project);
        });

        // clone the current project and test
        const project = await firstValueFrom(projectService.cloneProject('newTestName'));
        await expect(project.toDict()).toEqual(
            new Project({
                id: 'dddddddd-dddd-4ddd-addd-dddddddddddd',
                name: 'newTestName',
                description: 'Default project',
                spatialReference: WGS_84.spatialReference,
                time: new Time(moment.utc('2014-04-01 12:00:00')),
                bbox: {lowerLeftCoordinate: {x: -180, y: -90}, upperRightCoordinate: {x: 180, y: 90}},
                plots: [],
                layers: [],
                timeStepDuration: {
                    durationAmount: 1,
                    durationUnit: 'month',
                },
                version: {
                    changed: project.version.changed,
                    id: '0',
                },
            }).toDict(),
        );
    });

    it('#getProjectStream should return project stream', async () => {
        const project = await firstValueFrom(
            projectService.createDefaultProject().pipe(
                tap((theProject) => projectService.setProject(theProject)),
                mergeMap(() => projectService.getProjectStream().pipe(first())),
                mergeMap(async (theProject) => {
                    await expect(theProject.toDict()).toEqual(
                        new Project({
                            id: 'dddddddd-dddd-4ddd-addd-dddddddddddd',
                            name: 'Default',
                            description: 'Default project',
                            spatialReference: WGS_84.spatialReference,
                            time: new Time(moment.utc('2014-04-01 12:00:00')),
                            bbox: {lowerLeftCoordinate: {x: -180, y: -90}, upperRightCoordinate: {x: 180, y: 90}},
                            plots: [],
                            layers: [],
                            timeStepDuration: {
                                durationAmount: 1,
                                durationUnit: 'month',
                            },
                            version: {
                                changed: theProject.version.changed,
                                id: '0',
                            },
                        }).toDict(),
                    );

                    return theProject;
                }),
                mergeMap(() =>
                    projectService.createProject({
                        name: 'testProject',
                        description: 'testDescription',
                        spatialReference: WGS_84.spatialReference,
                        time: new Time(moment.utc('2021-07-04 11:00:00')),
                        bounds: {lowerLeftCoordinate: {x: -180, y: -90}, upperRightCoordinate: {x: 180, y: 90}},
                        timeStepDuration: {
                            durationAmount: 1,
                            durationUnit: 'month',
                        },
                    }),
                ),
                tap((theProject) => projectService.setProject(theProject)),
                mergeMap(() => projectService.getProjectOnce()),
            ),
        );
        await expect(project.toDict()).toEqual(
            new Project({
                id: 'dddddddd-dddd-4ddd-addd-dddddddddddd',
                name: 'testProject',
                description: 'testDescription',
                spatialReference: WGS_84.spatialReference,
                time: new Time(moment.utc('2021-07-04 11:00:00')),
                bbox: {lowerLeftCoordinate: {x: -180, y: -90}, upperRightCoordinate: {x: 180, y: 90}},
                plots: [],
                layers: [],
                timeStepDuration: {
                    durationAmount: 1,
                    durationUnit: 'month',
                },
                version: {
                    changed: project.version.changed,
                    id: '0',
                },
            }).toDict(),
        );
    });

    it('#getProjectOnce should return current project and #setProject should set a project', async () => {
        const project = await firstValueFrom(
            projectService.createDefaultProject().pipe(
                tap((theProject) => projectService.setProject(theProject)),
                mergeMap(() => projectService.getProjectOnce()),
                mergeMap(async (theProject) => {
                    await expect(theProject.toDict()).toEqual(
                        new Project({
                            id: 'dddddddd-dddd-4ddd-addd-dddddddddddd',
                            name: 'Default',
                            description: 'Default project',
                            spatialReference: WGS_84.spatialReference,
                            time: new Time(moment.utc('2014-04-01 12:00:00')),
                            bbox: {lowerLeftCoordinate: {x: -180, y: -90}, upperRightCoordinate: {x: 180, y: 90}},
                            plots: [],
                            layers: [],
                            timeStepDuration: {
                                durationAmount: 1,
                                durationUnit: 'month',
                            },
                            version: {
                                changed: theProject.version.changed,
                                id: '0',
                            },
                        }).toDict(),
                    );

                    return theProject;
                }),
                mergeMap(() =>
                    projectService.createProject({
                        name: 'testProject',
                        description: 'testDescription',
                        spatialReference: WGS_84.spatialReference,
                        time: new Time(moment.utc('2021-07-04 11:00:00')),
                        bounds: {lowerLeftCoordinate: {x: -180, y: -90}, upperRightCoordinate: {x: 180, y: 90}},
                        timeStepDuration: {
                            durationAmount: 1,
                            durationUnit: 'month',
                        },
                    }),
                ),
                tap((theProject) => projectService.setProject(theProject)),
                mergeMap(() => projectService.getProjectOnce()),
            ),
        );
        await expect(project.toDict()).toEqual(
            new Project({
                id: 'dddddddd-dddd-4ddd-addd-dddddddddddd',
                name: 'testProject',
                description: 'testDescription',
                spatialReference: WGS_84.spatialReference,
                time: new Time(moment.utc('2021-07-04 11:00:00')),
                bbox: {lowerLeftCoordinate: {x: -180, y: -90}, upperRightCoordinate: {x: 180, y: 90}},
                plots: [],
                layers: [],
                timeStepDuration: {
                    durationAmount: 1,
                    durationUnit: 'month',
                },
                version: {
                    changed: project.version.changed,
                    id: '0',
                },
            }).toDict(),
        );
    });

    describe('#createQueryAbortStream', () => {
        const tileExtent: Extent = [0, 0, 10, 10];

        let viewportSize$: BehaviorSubject<ViewportSize>;

        const makeProject = (time: Time, layers: Array<RasterLayer> = []): Project =>
            new Project({
                id: 'dddddddd-dddd-4ddd-addd-dddddddddddd',
                name: 'test',
                description: 'test',
                spatialReference: WGS_84.spatialReference,
                time,
                bbox: {lowerLeftCoordinate: {x: -180, y: -90}, upperRightCoordinate: {x: 180, y: 90}},
                plots: [],
                layers,
                timeStepDuration: {
                    durationAmount: 1,
                    durationUnit: 'month',
                },
                version: {
                    changed: new Date(),
                    id: '0',
                },
            });

        const makeLayer = (): RasterLayer =>
            new RasterLayer({
                id: 1,
                name: 'test',
                workflowId: 'ffffffff-ffff-4fff-afff-ffffffffffff',
                isVisible: true,
                isLegendVisible: true,
                symbology: {opacity: 1} as RasterSymbology,
            });

        beforeEach(() => {
            viewportSize$ = new BehaviorSubject<ViewportSize>({
                extent: [0, 0, 100, 100],
                resolution: 10,
            });
            mapServiceSpy.getViewportSizeStream.mockReturnValue(viewportSize$);

            projectService.setProject(makeProject(new Time(moment.utc('2014-04-01 12:00:00'))));
        });

        it('does not abort while the tile stays in the viewport', () => {
            let aborted = false;
            const subscription = projectService.createQueryAbortStream(1, tileExtent).subscribe(() => (aborted = true));

            // a pan that keeps the tile visible must not cancel the request
            viewportSize$.next({extent: [5, 5, 105, 105], resolution: 10});

            expect(aborted).toBe(false);
            subscription.unsubscribe();
        });

        it('aborts once a pan moves the tile out of the viewport', () => {
            let aborted = false;
            const subscription = projectService.createQueryAbortStream(1, tileExtent).subscribe(() => (aborted = true));

            // harmless pan first
            viewportSize$.next({extent: [5, 5, 105, 105], resolution: 10});
            expect(aborted).toBe(false);

            // then a pan that fully removes the tile
            viewportSize$.next({extent: [100, 100, 200, 200], resolution: 10});

            expect(aborted).toBe(true);
            subscription.unsubscribe();
        });

        it('aborts on zoom', () => {
            let aborted = false;
            const subscription = projectService.createQueryAbortStream(1, tileExtent).subscribe(() => (aborted = true));

            viewportSize$.next({extent: [0, 0, 100, 100], resolution: 20});

            expect(aborted).toBe(true);
            subscription.unsubscribe();
        });

        it('aborts when the project time changes', () => {
            let aborted = false;
            const subscription = projectService.createQueryAbortStream(1, tileExtent).subscribe(() => (aborted = true));

            projectService.setProject(makeProject(new Time(moment.utc('2014-04-02 12:00:00'))));

            expect(aborted).toBe(true);
            subscription.unsubscribe();
        });

        it('aborts when the layer is removed from the project', () => {
            projectService.setProject(makeProject(new Time(moment.utc('2014-04-01 12:00:00')), [makeLayer()]));

            let aborted = false;
            const subscription = projectService.createQueryAbortStream(1, tileExtent).subscribe(() => (aborted = true));

            // setting a project without the layer completes its layer stream
            projectService.setProject(makeProject(new Time(moment.utc('2014-04-01 12:00:00'))));

            expect(aborted).toBe(true);
            subscription.unsubscribe();
        });
    });
});
