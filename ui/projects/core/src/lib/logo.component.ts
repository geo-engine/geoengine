import {Component} from '@angular/core';

@Component({
    selector: 'geoengine-vat-logo',
    template: ` <h1><span class="blue">V</span><span class="light-blue">A</span><span class="green">T</span></h1> `,
    styles: [
        `
            h1 {
                background: #fff;
                height: 3rem;
                padding: 0.5rem;
                border-radius: 3px;
                margin: calc((82px - 4rem) / 2) auto;
            }

            .blue {
                color: #3258a1;
            }

            .light-blue {
                color: #3cace4;
            }

            .green {
                color: #8cb74c;
            }
        `,
    ],
})
export class VatLogoComponent {}
