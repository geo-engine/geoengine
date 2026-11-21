import {TestBed} from '@angular/core/testing';
import {currentCompletions, startCompletion} from '@codemirror/autocomplete';
import {EditorView} from '@codemirror/view';
import {describe, expect, it, vi} from 'vitest';
import {CodeEditorComponent} from './code-editor.component';

describe('CodeEditorComponent', () => {
    function createEditor(): {fixture: ReturnType<typeof TestBed.createComponent<CodeEditorComponent>>; view: EditorView} {
        const fixture = TestBed.createComponent(CodeEditorComponent);
        fixture.componentRef.setInput('language', 'Rust');
        fixture.detectChanges();

        const element = (fixture.nativeElement as HTMLElement).querySelector<HTMLElement>('.cm-editor');
        const view = element ? EditorView.findFromDOM(element) : null;
        if (!view) {
            throw new Error('editor view should exist after rendering');
        }

        return {fixture, view};
    }

    async function completionLabels(view: EditorView, text: string): Promise<Array<string>> {
        view.dispatch({changes: {from: 0, to: view.state.doc.length, insert: text}, selection: {anchor: text.length}});
        startCompletion(view);

        await vi.waitFor(() => expect(currentCompletions(view.state).length).toBeGreaterThan(0));

        return currentCompletions(view.state).map((completion) => completion.label);
    }

    it('suggests completions that are set after rendering', async () => {
        const {fixture, view} = createEditor();

        fixture.componentRef.setInput('completions', [{label: 'max'}]);
        fixture.detectChanges();
        expect(await completionLabels(view, 'm')).toEqual(['max']);

        fixture.componentRef.setInput('completions', [{label: 'max'}, {label: 'nir'}]);
        fixture.detectChanges();
        expect(await completionLabels(view, 'n')).toEqual(['nir']);
    });

    it('indents with tab', () => {
        const {view} = createEditor();

        view.contentDOM.dispatchEvent(new KeyboardEvent('keydown', {key: 'Tab', bubbles: true, cancelable: true}));

        expect(view.state.doc.toString()).not.toEqual('');
    });

    it('keeps escape inside the editor', () => {
        const {fixture, view} = createEditor();

        const onParentKeydown = vi.fn();
        (fixture.nativeElement as HTMLElement).addEventListener('keydown', onParentKeydown);

        view.contentDOM.dispatchEvent(new KeyboardEvent('keydown', {key: 'Escape', bubbles: true, cancelable: true}));
        expect(onParentKeydown).not.toHaveBeenCalled();

        view.contentDOM.dispatchEvent(new KeyboardEvent('keydown', {key: 'a', bubbles: true, cancelable: true}));
        expect(onParentKeydown).toHaveBeenCalledTimes(1);
    });
});
