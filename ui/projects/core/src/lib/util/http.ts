import {HttpHeaders} from '@angular/common/http';

/**
 * Extracts the filename from HTTP header's Content-Disposition field.
 */
export function filenameFromHttpHeaders(headers: HttpHeaders): string | undefined {
    const contentDisposition = headers.get('Content-Disposition');

    if (!contentDisposition) {
        return undefined;
    }

    for (const [key, value] of contentDisposition.split(';').map((s) => s.trim().split('='))) {
        if (key === 'filename') {
            // remove quotes from start/end if present
            return value.replace(/^"(.+(?="$))"$/, '$1');
        }
    }

    return undefined;
}
