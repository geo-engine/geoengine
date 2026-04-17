import {Component, OnInit, inject} from '@angular/core';
import {MAT_DIALOG_DATA, MatDialogContent} from '@angular/material/dialog';
import {DialogHeaderComponent} from '../../../dialogs/dialog-header/dialog-header.component';
import {CdkScrollable} from '@angular/cdk/scrolling';
import {MatTooltip} from '@angular/material/tooltip';
import {MatMiniFabButton} from '@angular/material/button';
import {MatIcon} from '@angular/material/icon';
import {MediaviewPlaylistComponent} from '../playlist/mediaview.playlist.component';

@Component({
    selector: 'geoengine-mediaview-dialog',
    templateUrl: './mediaview.dialog.component.html',
    styleUrls: ['./mediaview.dialog.component.scss'],
    imports: [DialogHeaderComponent, CdkScrollable, MatDialogContent, MatTooltip, MatMiniFabButton, MatIcon, MediaviewPlaylistComponent],
})

/**
 * Dialog-Media-Component
 * Is shown as a dialog-popup.
 * Displays a media-gallery. One media(image, audio, video) is shown at a time.
 * The component receives an array of urls to media files, the first media and an array specifying the types of the media files.
 */
export class MediaviewDialogComponent implements OnInit {
    data = inject<{
        mediaURLs: string[];
        currentMedia: number;
        mediaTypes: string[];
    }>(MAT_DIALOG_DATA);

    mediaURLs: Array<string> = [];

    currentMedia!: number;

    mediaTypes: Array<string> = [];

    ngOnInit(): void {
        this.mediaURLs = this.data.mediaURLs;
        this.currentMedia = this.data.currentMedia;
        this.mediaTypes = this.data.mediaTypes;
    }

    get mediaNames(): Array<string> {
        const mediaNames = new Array<string>();
        for (const media of this.mediaURLs) {
            mediaNames.push(media?.split('/').pop() ?? '');
        }
        return mediaNames;
    }

    get mediaDialogHeader(): string {
        return (
            this.mediaTypes[this.currentMedia]?.charAt(0).toUpperCase() +
            this.mediaTypes[this.currentMedia]?.slice(1) +
            ' ' +
            (this.currentMedia + 1) +
            ' of ' +
            this.mediaURLs.length
        );
    }

    /**
     * Plays the media with the given id.
     */
    goToMedia(mediaID: number): void {
        this.currentMedia = mediaID;
    }

    nextMedia(): void {
        this.currentMedia = (this.currentMedia + 1) % this.mediaURLs.length;
    }

    previousMedia(): void {
        this.currentMedia = this.currentMedia <= 0 ? this.mediaURLs.length - 1 : this.currentMedia - 1;
    }

    openInNewTab(): void {
        window.open(this.mediaURLs[this.currentMedia], '_blank', 'noopener');
    }
}
