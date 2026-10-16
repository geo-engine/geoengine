/* eslint-disable @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-member-access */ // TODO: fix these linting errors
import {AppPage} from './app.po';
import {browser, logging} from 'protractor';

describe('workspace-project App', () => {
    let page: AppPage;

    beforeEach(() => {
        page = new AppPage();
    });

    it('should display welcome message', async (): Promise<void> => {
        await page.navigateTo();
        await expect(await page.getTitleText()).toEqual('geoengine app is running!');
    });

    afterEach(async (): Promise<void> => {
        // Assert that there are no errors emitted from the browser
        const logs = (await browser.manage().logs().get(logging.Type.BROWSER)) as logging.Entry[];
        await expect(logs).not.toContain(
            jasmine.objectContaining({
                level: logging.Level.SEVERE as unknown,
            }),
        );
    });
});
