import {Component, ChangeDetectionStrategy, AfterViewInit, ElementRef, input, signal, viewChild, computed, effect} from '@angular/core';
import {NG_VALUE_ACCESSOR, ControlValueAccessor} from '@angular/forms';
import {acceptCompletion, autocompletion, Completion, completeFromList} from '@codemirror/autocomplete';
import {indentWithTab} from '@codemirror/commands';
import {json} from '@codemirror/lang-json';
import {rust} from '@codemirror/lang-rust';
import {syntaxHighlighting, defaultHighlightStyle} from '@codemirror/language';
import {Compartment, EditorState, Extension, Prec} from '@codemirror/state';
import {EditorView, ViewUpdate, lineNumbers, drawSelection, keymap, Panel, showPanel, PanelConstructor} from '@codemirror/view';

type Language = 'Rust' | 'JSON';

/**
 * A wrapper for the code editor.
 */
@Component({
    selector: 'geoengine-code-editor',
    template: `<div #editor></div>`,
    styles: [
        `
            :host {
                display: block;
            }

            div {
                height: 100%;
                width: 100%;
            }
        `,
    ],
    providers: [
        {
            provide: NG_VALUE_ACCESSOR,
            useExisting: CodeEditorComponent,
            multi: true,
        },
    ],
    changeDetection: ChangeDetectionStrategy.OnPush,
})
export class CodeEditorComponent implements ControlValueAccessor, AfterViewInit {
    private readonly editorRef = viewChild.required<ElementRef<HTMLDivElement>>('editor');

    readonly language = input.required<Language>();
    readonly header = input<string>();
    readonly readonly = input<boolean>(false);
    /** Suggestions for autocompletion. No autocompletion if empty. */
    readonly completions = input<ReadonlyArray<Completion>>([]);

    private writtenCode = '';
    private pristine = true;
    private readonly code = signal<string>(this.writtenCode);

    private readonly languageMode = computed(() => {
        switch (this.language()) {
            case 'Rust':
                return rust();
            case 'JSON':
                return json();
        }
    });

    private editor?: EditorView;
    private readonly languageCompartment = new Compartment();
    private readonly headerCompartment = new Compartment();
    private readonly readonlyCompartment = new Compartment();
    private readonly completionCompartment = new Compartment();
    private readonly headerPanel: Panel = createHeaderPanel();

    private onChanged?: (code: string) => void;
    private onTouched?: () => void;

    constructor() {
        // Read signals before `this.editor?.`: optional chaining skips the arguments while the editor does not exist,
        // so the effect would not depend on the signals and never run again.

        effect(() => {
            const languageMode = this.languageMode();
            this.editor?.dispatch({
                effects: this.languageCompartment.reconfigure(languageMode),
            });
        });

        effect(() => {
            const code = this.code();

            if (!this.onChanged) return;
            if (this.pristine && this.writtenCode === code) return; // don't call onChanged if the code has not changed

            this.pristine = false;

            this.onChanged(code);
        });

        effect(() => {
            const header = this.header();

            this.headerPanel.dom.textContent = header ?? null;

            const panelConstructor: PanelConstructor | null = header ? (): Panel => this.headerPanel : null;

            this.editor?.dispatch({
                effects: this.headerCompartment.reconfigure(showPanel.of(panelConstructor)),
            });
        });

        effect(() => {
            const readonly = this.readonly();
            this.editor?.dispatch({effects: this.readonlyCompartment.reconfigure(EditorState.readOnly.of(readonly))});
        });

        effect(() => {
            const completions = this.completions();
            this.editor?.dispatch({effects: this.completionCompartment.reconfigure(completionExtension(completions))});
        });
    }

    ngAfterViewInit(): void {
        const minHeightEditor = EditorView.theme({
            // eslint-disable-next-line @typescript-eslint/naming-convention
            '.cm-content, .cm-gutter': {minHeight: '6rem'},
        });

        this.editor = new EditorView({
            parent: this.editorRef().nativeElement,
            doc: this.code(),
            extensions: [
                lineNumbers(),
                drawSelection(),
                this.languageCompartment.of(this.languageMode()),
                syntaxHighlighting(defaultHighlightStyle),
                this.headerCompartment.of(showPanel.of(null)),
                this.readonlyCompartment.of(EditorState.readOnly.of(this.readonly())),
                this.completionCompartment.of(completionExtension(this.completions())),
                // Tab accepts a suggestion or indents; Escape followed by Tab moves the focus out of the editor
                Prec.high(keymap.of([{key: 'Tab', run: acceptCompletion}])),
                keymap.of([indentWithTab]),
                // the editor handles Escape itself, so it must not close surrounding dialogs or sidenavs
                EditorView.domEventHandlers({
                    keydown: (event) => {
                        if (event.key === 'Escape') {
                            event.stopPropagation();
                        }
                        return false; // let the editor process the key
                    },
                }),
                minHeightEditor,
                EditorView.updateListener.of((v: ViewUpdate) => {
                    if (v.docChanged) {
                        this.code.set(v.state.doc.toString());
                    }
                    if (v.focusChanged && this.onTouched) {
                        this.onTouched();
                    }
                }),
            ],
        });
    }

    /** Implemented as part of ControlValueAccessor. */
    writeValue(code: unknown): void {
        if (typeof code !== 'string') return;

        this.writtenCode = code;
        this.pristine = true;

        if (this.editor) {
            this.editor?.dispatch({
                changes: {from: 0, to: this.editor.state.doc.length, insert: code},
            });
        } else {
            this.code.set(code);
        }
    }

    /** Implemented as part of ControlValueAccessor. */
    registerOnChange(fn: (_: unknown) => unknown): void {
        this.onChanged = fn;
    }

    /** Implemented as part of ControlValueAccessor. */
    registerOnTouched(fn: () => void): void {
        this.onTouched = fn;
    }
}

function completionExtension(completions: ReadonlyArray<Completion>): Extension {
    if (completions.length === 0) {
        return [];
    }
    return autocompletion({override: [completeFromList([...completions])]});
}

function createHeaderPanel(): Panel {
    const dom = document.createElement('div');
    dom.textContent = 'F1: Toggle the help panel';
    dom.style.fontFamily = 'monospace';
    dom.style.padding = '0.5rem';
    return {top: true, dom};
}
