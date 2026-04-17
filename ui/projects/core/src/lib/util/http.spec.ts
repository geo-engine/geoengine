import {HttpHeaders} from '@angular/common/http';
import {filenameFromHttpHeaders} from './http';

describe('Http Utils', () => {
    it('extracts the filename from headers', () => {
        expect(
            filenameFromHttpHeaders(
                new HttpHeaders().set('Content-Disposition', 'attachment; filename="metadata_69cf7aaf-e828-537e-be9b-31933120c931.zip"'),
            ),
        ).toBe('metadata_69cf7aaf-e828-537e-be9b-31933120c931.zip');

        expect(
            filenameFromHttpHeaders(new HttpHeaders().set('Content-Disposition', 'form-data; name="fieldName"; filename="filename.jpg"')),
        ).toBe('filename.jpg');

        expect(filenameFromHttpHeaders(new HttpHeaders().set('Content-Disposition', 'attachment'))).toBeUndefined();

        expect(filenameFromHttpHeaders(new HttpHeaders().set('Content-Disposition', 'form-data; name="fieldName"'))).toBeUndefined();
    });
});
