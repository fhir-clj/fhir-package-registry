//https://dev.to/rohanfaiyazkhan/how-to-make-an-accesible-auto-suggest-with-vanilla-javascript-4g27
//build purejs combobox for htmx

class MyComponent extends HTMLElement {
  constructor() {
    super()
    this.attachShadow({mode: 'open'})
  }

  // Lifecycle hooks
  connectedCallback() {
    this.render()
  }

  // Watched attributes
  static get observedAttributes() {
    return ['title']
  }

  attributeChangedCallback(name, oldVal, newVal) {
    this.render()
  }

  render() {
    this.shadowRoot.innerHTML = `
      <style>
        :host {
          display: block;
          padding: 10px;
        }
      </style>
      <div>
        <h1 class="text-red-500">${this.getAttribute('title')}</h1>
        <slot></slot>
      </div>
    `
  }
}

// Register
console.log('register');
customElements.define('my-component', MyComponent)
