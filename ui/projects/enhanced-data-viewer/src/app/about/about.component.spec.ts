import {ComponentFixture, TestBed} from '@angular/core/testing';
import {describe, expect, it} from 'vitest';
import {AboutComponent} from './about.component';

describe('AboutComponent', () => {
    let fixture: ComponentFixture<AboutComponent>;

    it('renders the app overview and key capabilities', async () => {
        await TestBed.configureTestingModule({
            imports: [AboutComponent],
        }).compileComponents();

        fixture = TestBed.createComponent(AboutComponent);
        fixture.detectChanges();

        const host = fixture.nativeElement as HTMLElement;

        expect(host.textContent).toContain('Enhanced Data Viewer');
        expect(host.textContent).toContain('Explore data layers');
        expect(host.textContent).toContain('CODE-DE Lab');
    });
});
