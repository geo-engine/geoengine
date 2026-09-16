import {ChangeDetectionStrategy, Component} from '@angular/core';
import {MatDividerModule} from '@angular/material/divider';

@Component({
    selector: 'geoengine-about',
    imports: [MatDividerModule],
    template: `
        <section>
            <!-- <span class="eyebrow">About the Enhanced Data Viewer</span> -->
            <h2>What this viewer does</h2>
            <p>
                The <em>Enhanced Data Viewer</em> brings earth observation and geospatial datasets into a focused workspace for fast, visual
                exploration. Browse and inspect layers, and compare their spatial context without losing track of the map.
            </p>
        </section>

        <mat-divider></mat-divider>

        <section>
            <h2>Key capabilities</h2>
            <ul>
                <li>Explore data layers from the <a href="https://code-de.org/">CODE-DE Lab</a> ecosystem</li>
                <li>Get daily aggregated data from multiple scenes</li>
                <li>Visualize data with various visualization presets</li>
                <li>Analyze bands with compute tools</li>
            </ul>
        </section>

        <mat-divider></mat-divider>

        <section>
            <h2>The Geo Engine</h2>
            <p>
                Geo Engine is the underlying geospatial platform that provides data access, processing, and visualization capabilities for
                this viewer. It brings together raster and vector workflows, cloud-ready processing, and map-based exploration in one shared
                backend.
            </p>
            <ul>
                <li>The technology: <a href="https://geoengine.io" target="_blank" rel="noreferrer">geoengine.io</a></li>
                <li>Documentation: <a href="https://docs.geoengine.io" target="_blank" rel="noreferrer">Docs</a></li>
            </ul>
        </section>
    `,
    styles: [
        `
            :host {
                width: 100%;
                box-sizing: border-box;
                display: block;
                padding: 0.5rem 0.5rem 2rem 0.5rem;
                color: var(--mat-sys-on-surface);
            }

            section {
                width: 100%;
            }

            mat-divider {
                margin: 0.5rem 0;
            }

            h2 {
                margin: 0 0 0.5rem;
                font-size: 1rem;
                font-weight: 600;
                color: var(--mat-sys-on-surface);
            }

            p,
            li {
                margin: 0;
                line-height: 1.6;
                color: var(--mat-sys-on-surface-variant);
            }

            ul {
                margin: 0.5rem 0 0;
                padding-left: 1.1rem;
            }

            li + li {
                margin-top: 0.25rem;
            }
        `,
    ],
    changeDetection: ChangeDetectionStrategy.OnPush,
    standalone: true,
})
export class AboutComponent {}
