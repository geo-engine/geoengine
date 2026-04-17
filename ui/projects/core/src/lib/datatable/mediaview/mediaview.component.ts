import {Component, OnInit, ChangeDetectionStrategy, inject, input} from '@angular/core';
import {MediaviewDialogComponent} from './dialog/mediaview.dialog.component';
import {MatDialog} from '@angular/material/dialog';
import {VectorColumnDataType, VectorColumnDataTypes} from '@geoengine/common';
import {MatIconButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';

@Component({
    selector: 'geoengine-datatable-mediaview',
    templateUrl: './mediaview.component.html',
    styleUrls: ['./mediaview.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    imports: [MatIconButton, MatIcon],
})

/**
 * Dialog-Component
 * Checks the file-type of the comma-separated urls given as input-argument and sets up links to open a dialog.
 * The dialog will show the images or play the audios or videos.
 */
export class MediaviewComponent implements OnInit {
    private readonly mediadialog = inject(MatDialog);

    mediaType: Array<string> = [];
    mediaUrls: Array<string> = [];

    readonly url = input<string>();

    readonly type = input.required<VectorColumnDataType>();

    private urls: Array<string> = [];

    /**
     * Extracts the type (image, audio, video) of a given file-url string.
     */
    public static getType(value: string): string {
        let ret: string;
        if (!value || value === '') return (ret = '');
        const fileSplits = value.split('.') ?? [];
        if (fileSplits.length <= 1) return (ret = '');
        const fileEnding = fileSplits.pop()?.toLowerCase() ?? '';
        const imageArray = ['jpg', 'jpeg', 'gif', 'png', 'svg', 'bmp'];
        const audioArray = ['wav', 'mp3', 'ogg', 'aac'];
        const videoArray = ['webm', 'mp4', 'ogv'];

        const isMediaFile = imageArray.includes(fileEnding)
            ? (ret = 'image')
            : audioArray.includes(fileEnding)
              ? (ret = 'audio')
              : videoArray.includes(fileEnding)
                ? (ret = 'video')
                : (ret = 'text');
        if (!isMediaFile) ret = 'text';
        return ret;
    }

    /**
     * Gets the urls and file-types of the comma-separated urls given as input-argument.
     */
    ngOnInit(): void {
        const url = this.url();
        if (!url) {
            this.urls = [];
            this.mediaType = [];
            this.mediaUrls = [];
            return;
        }

        if (this.type() === VectorColumnDataTypes.Media) {
            this.urls = url.split(',');
            this.mediaType = [];
            this.mediaUrls = [];

            // eslint-disable-next-line @typescript-eslint/no-for-in-array
            for (const i in this.urls) {
                if (Object.hasOwn(this.urls, i)) {
                    const checkMediaType = MediaviewComponent.getType(this.urls[i]);
                    if (checkMediaType !== '' && checkMediaType !== 'text') {
                        this.urls[i] = this.urls[i].trim();
                        this.mediaType.push(checkMediaType);
                        this.mediaUrls.push(this.urls[i]);
                    }
                }
            }
        } else {
            this.urls = [url.toString()];
            this.mediaType = [''];
            this.mediaUrls = [url.toString()];
        }
    }

    get media(): string {
        const differentMediaTypes = this.mediaType.filter((item, index) => !(this.mediaType.indexOf(item) !== index));
        if (differentMediaTypes?.length > 1) {
            return 'media';
        } else {
            return this.mediaType[0] ?? '';
        }
    }

    /**
     * Opens the media in a new dialog window.
     */
    public openMediaviewDialog(mediaID: number): void {
        this.mediadialog.open(MediaviewDialogComponent, {
            height: '80vh',
            width: '80vw',
            disableClose: true,
            panelClass: 'mediaviewDialogContainer',
            data: {mediaURLs: this.mediaUrls, currentMedia: mediaID, mediaTypes: this.mediaType},
        });
    }
}
