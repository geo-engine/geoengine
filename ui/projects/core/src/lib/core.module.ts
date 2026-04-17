import {NgModule} from '@angular/core';
import {MatAutocompleteModule} from '@angular/material/autocomplete';
import {MatButtonModule} from '@angular/material/button';
import {MatButtonToggleModule} from '@angular/material/button-toggle';
import {MatCardModule, MAT_CARD_CONFIG} from '@angular/material/card';
import {MatCheckboxModule} from '@angular/material/checkbox';
import {MatChipsModule} from '@angular/material/chips';
import {MatDatepickerModule} from '@angular/material/datepicker';
import {MatDialogModule} from '@angular/material/dialog';
import {MatExpansionModule} from '@angular/material/expansion';
import {MatGridListModule} from '@angular/material/grid-list';
import {MatIconModule} from '@angular/material/icon';
import {MatInputModule} from '@angular/material/input';
import {MatListModule} from '@angular/material/list';
import {MatMenuModule} from '@angular/material/menu';
import {MatPaginatorModule} from '@angular/material/paginator';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import {MatRadioModule} from '@angular/material/radio';
import {MatSelectModule} from '@angular/material/select';
import {MatSidenavModule} from '@angular/material/sidenav';
import {MatSliderModule} from '@angular/material/slider';
import {MatSlideToggleModule} from '@angular/material/slide-toggle';
import {MatSnackBarModule} from '@angular/material/snack-bar';
import {MatStepperModule} from '@angular/material/stepper';
import {MatTableModule} from '@angular/material/table';
import {MatTabsModule} from '@angular/material/tabs';
import {MatToolbarModule} from '@angular/material/toolbar';
import {MatTooltipModule} from '@angular/material/tooltip';
import {DragDropModule} from '@angular/cdk/drag-drop';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';
import {provideHttpClient, withInterceptorsFromDi} from '@angular/common/http';
import {DialogHeaderComponent} from './dialogs/dialog-header/dialog-header.component';
import {DialogSectionHeadingComponent} from './dialogs/dialog-section-heading/dialog-section-heading.component';
import {VatLogoComponent} from './logo.component';
import {LoginComponent} from './users/login/login.component';
import {SafeHtmlPipe} from './util/pipes/safe-html.pipe';
import {TrimPipe} from './util/pipes/trim.pipe';
import {CssStringToRgbaPipe} from './util/pipes/css-string-to-rgba.pipe';
import {HighlightPipe} from './util/pipes/highlight.pipe';
import {RgbaToCssStringPipe} from './util/pipes/rgba-to-css-string.pipe';
import {CommonModule as AngularCommonModule} from '@angular/common';
import {DialogHelpComponent} from './dialogs/dialog-help/dialog-help.component';
import {SidenavHeaderComponent} from './sidenav/sidenav-header/sidenav-header.component';
import {SidenavContainerComponent} from './sidenav/sidenav-container/sidenav-container.component';
import {NavigationComponent} from './sidenav/navigation/navigation.component';
import {SidenavSearchComponent, SidenavSearchRightDirective} from './sidenav/sidenav-search/sidenav-search.component';
import {ZoomHandlesComponent} from './map/zoom-handles/zoom-handles.component';
import {MapContainerComponent} from './map/map-container/map-container.component';
import {OlRasterLayerComponent, OlVectorLayerComponent} from './map/map-layer.component';
import {RenameLayerComponent} from './layers/rename-layer/rename-layer.component';
import {VectorLegendComponent} from './layers/legend/legend-vector/vector-legend.component';
import {LayerListComponent} from './layers/layer-list/layer-list.component';
import {LayerListElementComponent} from './layers/layer-list/layer-list-element/layer-list-element.component';
import {RasterLegendComponent} from './layers/legend/legend-raster/raster-legend.component';
import {SafeStylePipe} from './util/pipes/safe-style.pipe';
import {SmallTimeInteractionComponent} from './time/small-time-interaction/small-time-interaction.component';
import {TimeConfigComponent} from './time/time-config/time-config.component';
import {WorkspaceSettingsComponent} from './project/workspace-settings/workspace-settings.component';
import {ChangeSpatialReferenceComponent} from './project/change-spatial-reference/change-spatial-reference.component';
import {IfGuestDirective} from './util/directives/if-guest.directive';
import {IfLoggedInDirective} from './util/directives/if-logged-in.directive';
import {NewProjectComponent} from './project/new-project/new-project.component';
import {LoadProjectComponent} from './project/load-project/load-project.component';
import {SaveProjectAsComponent} from './project/save-project-as/save-project-as.component';
import {MultiLayerSelectionComponent} from './operators/dialogs/helpers/multi-layer-selection/multi-layer-selection.component';
import {OperatorListComponent} from './operators/dialogs/operator-list/operator-list.component';
import {ExpressionOperatorComponent} from './operators/dialogs/expression-operator/expression-operator.component';
import {OperatorOutputNameComponent} from './operators/dialogs/helpers/operator-output-name/operator-output-name.component';
import {AddDataComponent} from './datasets/add-data/add-data.component';
import {DatasetListComponent} from './datasets/dataset-list/dataset-list.component';
import {DatasetComponent} from './datasets/dataset/dataset.component';
import {PlotListComponent} from './plots/plot-list/plot-list.component';
import {StatisticsPlotComponent} from './operators/dialogs/statistics-plot/statistics-plot.component';
import {PlotDetailViewComponent} from './plots/plot-detail-view/plot-detail-view.component';
import {PlotListEntryComponent} from './plots/plot-list-entry/plot-list-entry.component';
import {HistogramOperatorComponent} from './operators/dialogs/histogram-operator/histogram-operator.component';
import {BoxPlotOperatorComponent} from './operators/dialogs/boxplot-operator/boxplot-operator.component';
import {ScatterplotOperatorComponent} from './operators/dialogs/scatterplot-operator/scatterplot-operator.component';
import {LayerSelectionComponent} from './operators/dialogs/helpers/layer-selection/layer-selection.component';
import {LineageGraphComponent} from './provenance/lineage-graph/lineage-graph.component';
import {MeanRasterPixelValuesOverTimeDialogComponent} from './operators/dialogs/mean-raster-pixel-values-over-time-dialog/mean-raster-pixel-values-over-time-dialog.component';
import {RasterVectorJoinComponent} from './operators/dialogs/raster-vector-join/raster-vector-join.component';
import {RasterStackerComponent} from './operators/dialogs/raster-stacker/raster-stacker.component';
import {RasterTypeConversionComponent} from './operators/dialogs/raster-type-conversion/raster-type-conversion.component';
import {RasterScalingComponent} from './operators/dialogs/raster-scaling/raster-scaling.component';
import {PointInPolygonFilterOperatorComponent} from './operators/dialogs/point-in-polygon-filter/point-in-polygon-filter.component';
import {UploadComponent} from './datasets/upload/upload.component';
import {DataTableComponent} from './datatable/table/table.component';
import {TabsComponent} from './tabs/tabs.component';
import {PortalModule} from '@angular/cdk/portal';
import {DrawFeaturesComponent} from './datasets/draw-features/draw-features.component';
import {FeatureAttributeOvertimeComponent} from './operators/dialogs/feature-attribute-over-time/feature-attribute-over-time.component';
import {NotificationsComponent} from './project/notifications/notifications.component';
import {TemporalRasterAggregationComponent} from './operators/dialogs/temporal-raster-aggregation/temporal-raster-aggregation.component';
import {DragAndDropComponent} from './datasets/drag-and-drop/drag-and-drop.component';
import {AddWorkflowComponent} from './datasets/add-workflow/add-workflow.component';
import {ProvenanceTableComponent} from './provenance/table/provenance-table.component';
import {ScrollingModule} from '@angular/cdk/scrolling';
import {LayerListMenuComponent} from './layers/layer-list/layer-list-menu/layer-list-menu.component';
import {ModalLoginComponent} from './users/modal-login/modal-login.component';
import {TimeStepSelectorComponent} from './time/time-step-selector/time-step-selector.component';
import {TokenLoginComponent} from './users/token-login/token-login.component';
import {TimeSliderComponent} from './time/time-slider/time-slider.component';
import {FullDisplayComponent} from './datatable/table/full-display/full-display.component';
import {ClassHistogramOperatorComponent} from './operators/dialogs/class-histogram-operator/class-histogram-operator.component';
import {ColumnRangeFilterComponent} from './operators/dialogs/column-range-filter/column-range-filter.component';
import {DialogSplashCheckboxComponent} from './dialogs/dialog-splash-checkbox/dialog-splash-checkbox.component';
import {MediaviewComponent} from './datatable/mediaview/mediaview.component';
import {MediaviewDialogComponent} from './datatable/mediaview/dialog/mediaview.dialog.component';
import {MediaviewPlaylistComponent} from './datatable/mediaview/playlist/mediaview.playlist.component';
import {OidcComponent} from './users/oidc/oidc.component';
import {InterpolationComponent} from './operators/dialogs/interpolation/interpolation.component';
import {DownsamplingComponent} from './operators/dialogs/downsampling/downsampling.component';
import {NeighborhoodAggregateComponent} from './operators/dialogs/neighborhood-aggregate/neighborhood-aggregate.component';
import {NotFoundPageComponent} from './util/components/not-found/not-found-page.component';
import {BackendStatusPageComponent} from './util/components/backend-status-page/backend-status-page.component';
import {SymbologyCreatorComponent} from './layers/symbology/symbology-creator/symbology-creator.component';
import {OperatorDialogContainerComponent} from './operators/dialogs/helpers/operator-dialog-container/operator-dialog-container.component';
import {MAT_FORM_FIELD_DEFAULT_OPTIONS} from '@angular/material/form-field';
import {TimeShiftComponent} from './operators/dialogs/time-shift/time-shift.component';
import {PieChartComponent} from './operators/dialogs/pie-chart/pie-chart.component';
import {RasterizationComponent} from './operators/dialogs/rasterization/rasterization.component';
import {UserSessionComponent} from './users/user-session/user-session.component';
import {QuotaInfoComponent} from './users/quota/quota-info/quota-info.component';
import {LineSimplificationComponent} from './operators/dialogs/line-simplification/line-simplification.component';
import {TaskListComponent} from './tasks/task-list/task-list.component';
import {RolesComponent} from './users/roles/roles.component';
import {VectorExpressionComponent} from './operators/dialogs/vector-expression/vector-expression.component';
import {CommonConfig, CommonModule} from '@geoengine/common';
import {SymbologyEditorComponent} from './layers/symbology/symbology-editor/symbology-editor.component';
import {MapResolutionExtentOverlayComponent} from './map/map-info/map-resolution-extent-overlay.component';
import {DownloadLayerComponent} from './download-layer/download-layer.component';
import {BandwiseExpressionOperatorComponent} from './operators/dialogs/bandwise-expression-operator/bandwise-expression-operator.component';
import {BandNeighborhoodAggregateComponent} from './operators/dialogs/band-neighborhood-aggregate/band-neighborhood-aggregate.component';
import {LayerCollectionSelectionComponent} from './layer-collections/layer-collection-selection.component';
import {CoreConfig} from './config.service';
import {BasemapSelectorComponent} from './project/basemap-selector/basemap-selector.component';

export const MATERIAL_MODULES = [
    MatAutocompleteModule,
    MatButtonModule,
    MatButtonToggleModule,
    MatCardModule,
    MatCheckboxModule,
    MatChipsModule,
    MatDatepickerModule,
    MatDialogModule,
    MatExpansionModule,
    MatExpansionModule,
    MatGridListModule,
    MatIconModule,
    MatInputModule,
    MatListModule,
    MatMenuModule,
    MatPaginatorModule,
    MatProgressBarModule,
    MatProgressSpinnerModule,
    MatRadioModule,
    MatSelectModule,
    MatSidenavModule,
    MatSlideToggleModule,
    MatSliderModule,
    MatSnackBarModule,
    MatStepperModule,
    MatTableModule,
    MatTabsModule,
    MatToolbarModule,
    MatTooltipModule,
];

const CORE_PIPES = [CssStringToRgbaPipe, HighlightPipe, RgbaToCssStringPipe, SafeHtmlPipe, SafeStylePipe, TrimPipe];

const CORE_COMPONENTS = [
    AddDataComponent,
    AddWorkflowComponent,
    BackendStatusPageComponent,
    BoxPlotOperatorComponent,
    ChangeSpatialReferenceComponent,
    BandNeighborhoodAggregateComponent,
    BandwiseExpressionOperatorComponent,
    ColumnRangeFilterComponent,
    DatasetComponent,
    DatasetListComponent,
    DataTableComponent,
    DialogHeaderComponent,
    DialogHelpComponent,
    DialogSectionHeadingComponent,
    DialogSplashCheckboxComponent,
    DownloadLayerComponent,
    DragAndDropComponent,
    DrawFeaturesComponent,
    ExpressionOperatorComponent,
    FeatureAttributeOvertimeComponent,
    FullDisplayComponent,
    HistogramOperatorComponent,
    InterpolationComponent,
    DownsamplingComponent,
    LayerCollectionSelectionComponent,
    LayerListComponent,
    LayerListElementComponent,
    LayerListMenuComponent,
    LineageGraphComponent,
    LineageGraphComponent,
    LineSimplificationComponent,
    LoadProjectComponent,
    LoginComponent,
    MapResolutionExtentOverlayComponent,
    MeanRasterPixelValuesOverTimeDialogComponent,
    MediaviewComponent,
    MediaviewDialogComponent,
    MediaviewPlaylistComponent,
    ModalLoginComponent,
    MultiLayerSelectionComponent,
    NavigationComponent,
    NeighborhoodAggregateComponent,
    NewProjectComponent,
    NotFoundPageComponent,
    NotificationsComponent,
    OidcComponent,
    OlRasterLayerComponent,
    OlVectorLayerComponent,
    OperatorListComponent,
    PieChartComponent,
    PlotDetailViewComponent,
    PlotListComponent,
    PlotListEntryComponent,
    PointInPolygonFilterOperatorComponent,
    ProvenanceTableComponent,
    QuotaInfoComponent,
    RasterizationComponent,
    RasterScalingComponent,
    RasterStackerComponent,
    RasterTypeConversionComponent,
    RasterVectorJoinComponent,
    RenameLayerComponent,
    RolesComponent,
    SaveProjectAsComponent,
    ScatterplotOperatorComponent,
    SidenavContainerComponent,
    SidenavSearchComponent,
    SidenavSearchRightDirective,
    SmallTimeInteractionComponent,
    StatisticsPlotComponent,
    SymbologyCreatorComponent,
    SymbologyEditorComponent,
    TabsComponent,
    TaskListComponent,
    TemporalRasterAggregationComponent,
    TimeConfigComponent,
    TimeShiftComponent,
    TimeSliderComponent,
    TimeStepSelectorComponent,
    TokenLoginComponent,
    UploadComponent,
    UserSessionComponent,
    VatLogoComponent,
    VectorExpressionComponent,
    VectorLegendComponent,
    ZoomHandlesComponent,
];

const CORE_COMPONENT_IMPORTS = [
    BasemapSelectorComponent,
    ClassHistogramOperatorComponent,
    IfGuestDirective,
    IfLoggedInDirective,
    LayerSelectionComponent,
    MapContainerComponent,
    OperatorDialogContainerComponent,
    OperatorOutputNameComponent,
    RasterLegendComponent,
    SidenavHeaderComponent,
    WorkspaceSettingsComponent,
];

@NgModule({
    exports: [
        /* re-exports */
        ...MATERIAL_MODULES,
        PortalModule,
        ReactiveFormsModule,
        ScrollingModule,
        /* library exports */
        ...CORE_PIPES,
        ...CORE_COMPONENTS,
        CommonModule,
    ],
    imports: [
        ...MATERIAL_MODULES,
        AngularCommonModule,
        CommonModule,
        DragDropModule,
        FormsModule,
        PortalModule,
        ReactiveFormsModule,
        ScrollingModule,
        ...CORE_COMPONENT_IMPORTS,
        ...CORE_PIPES,
        ...CORE_COMPONENTS,
    ],
    providers: [
        {provide: MAT_FORM_FIELD_DEFAULT_OPTIONS, useValue: {appearance: 'fill'}},
        {provide: MAT_CARD_CONFIG, useValue: {appearance: 'outlined'}},
        {provide: CommonConfig, useExisting: CoreConfig},
        provideHttpClient(withInterceptorsFromDi()),
    ],
})
export class CoreModule {}
