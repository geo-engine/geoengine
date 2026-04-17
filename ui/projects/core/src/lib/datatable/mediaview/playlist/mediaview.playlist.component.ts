import {Component, input, output} from '@angular/core';
import {MatNavList, MatListItem} from '@angular/material/list';
import {MatIcon} from '@angular/material/icon';
import {MatLine} from '@angular/material/grid-list';
import {MatTooltip} from '@angular/material/tooltip';

@Component({
    selector: 'geoengine-mediaview-playlist',
    templateUrl: './mediaview.playlist.component.html',
    styleUrls: ['./mediaview.playlist.component.scss'],
    imports: [MatNavList, MatListItem, MatIcon, MatLine, MatTooltip],
})

/**
 * Playlist-Component
 * Displays a playlist showing all files in the given list and highlights the file currently playing.
 * The component receives an array of track names and the id of the one currently playing (currentTrack) as inputs.
 * It has an Event-Emitter as Output that is fired when a playlist-item is clicked to be played
 */
export class MediaviewPlaylistComponent {
    readonly tracks = input<Array<string>>([]);
    readonly currentTrack = input.required<number>();

    /**
     * Output: Emitted when a link in the playlist is clicked to change the track. The track-id of the track to play is emitted
     */
    readonly gotoMediaP = output<number>();

    public goToMedia(trackID: number): void {
        this.gotoMediaP.emit(trackID);
    }
}
