import {beforeEach, describe, expect, it, vi} from 'vitest';
import {ComponentFixture, TestBed} from '@angular/core/testing';
import {Observable, of} from 'rxjs';
import {provideNativeDateAdapter} from '@angular/material/core';
import {ProjectService} from '@geoengine/core';
import {LayersService, Time, TimeStepDuration} from '@geoengine/common';
import {LayersComponent} from './layers.component';

describe('LayersComponent', () => {
    let fixture: ComponentFixture<LayersComponent>;
    const getLayerCollectionItems = vi.fn();
    const setTime = vi.fn().mockResolvedValue(undefined);
    const setTimeStepDuration = vi.fn();

    beforeEach(async () => {
        vi.clearAllMocks();
        getLayerCollectionItems.mockReset().mockResolvedValue({items: []});
        await TestBed.configureTestingModule({
            imports: [LayersComponent],
            providers: [
                provideNativeDateAdapter(),
                {provide: LayersService, useValue: {getLayerCollectionItems}},
                {
                    provide: ProjectService,
                    useValue: {
                        getTimeStream: (): Observable<Time> => of(new Time(new Date('2026-04-01T00:00:00Z'))),
                        getTimeStepDurationStream: (): Observable<TimeStepDuration> => of({durationAmount: 1, durationUnit: 'month'}),
                        setTime,
                        setTimeStepDuration,
                    },
                },
            ],
        }).compileComponents();
        fixture = TestBed.createComponent(LayersComponent);
    });

    it('shows harvested presets by default and exposes all categories in debug mode', async () => {
        fixture.detectChanges();
        await fixture.whenStable();
        expect(fixture.componentInstance.currentPresets().map((preset) => preset.category)).toEqual(['harvested', 'harvested']);
        expect((fixture.nativeElement as HTMLElement).querySelectorAll('.preset-group-label')).toHaveLength(0);

        fixture.componentRef.setInput('debug', true);
        fixture.detectChanges();
        await fixture.whenStable();
        expect(fixture.componentInstance.presetGroups().map((group) => group.category)).toEqual(['static', 'harvested', 'adHoc']);
        expect((fixture.nativeElement as HTMLElement).querySelectorAll('.preset-group-label')).toHaveLength(3);
    });

    it('finds a preset beyond the first collection page and updates the selected map layer', async () => {
        getLayerCollectionItems
            .mockResolvedValueOnce({items: Array.from({length: 20}, (_, index) => ({name: `Other ${index}`}))})
            .mockResolvedValueOnce({
                items: [{name: 'Sentinel-1 VV Band (Harvested)', id: {providerId: 'provider', layerId: 'vv'}}],
            });
        fixture.detectChanges();
        await fixture.whenStable();
        expect(getLayerCollectionItems).toHaveBeenNthCalledWith(2, expect.any(String), expect.any(String), 20, 20);
        expect(fixture.componentInstance.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'vv'});

        getLayerCollectionItems.mockResolvedValue({
            items: [{name: 'Sentinel-1 SAR False Color (Harvested)', id: {providerId: 'provider', layerId: 'false-color'}}],
        });
        const presets = (fixture.nativeElement as HTMLElement).querySelectorAll<HTMLElement>('.visualization-presets mat-list-item');
        presets[1].click();
        fixture.detectChanges();
        await fixture.whenStable();
        expect(fixture.componentInstance.mapTileLayer()).toEqual({dataConnectorId: 'provider', layerId: 'false-color'});
    });

    it('applies datasource time defaults and preserves the date when auto selection is disabled', async () => {
        fixture.detectChanges();
        await fixture.whenStable();
        const component = fixture.componentInstance;
        component.selectPreset(1);
        await component.setSelectedDataSource('landsat');
        expect(setTime).toHaveBeenLastCalledWith(new Time(new Date(1767916800000)));
        expect(setTimeStepDuration).toHaveBeenLastCalledWith({durationAmount: 1, durationUnit: 'day'});
        expect(component.selectedPresetIndex()).toBe(0);

        setTime.mockClear();
        component.autoSelectTime.set(false);
        await component.setSelectedDataSource('opengeohub-landsat');
        expect(setTime).not.toHaveBeenCalled();
        expect(setTimeStepDuration).toHaveBeenLastCalledWith({durationAmount: 2, durationUnit: 'months'});
        expect(component.selectedDataSource()).toBe('opengeohub-landsat');
    });
});
