var  tabs = {};
function open_tab(id, cmp_id) {
  var cur = tabs[cmp_id];
  //console.log('open-tab', id,cur)
  if( cur == id) { return; }
  var new_tab = document.getElementById(id);
  new_tab.style.display = 'block';
  var new_tab_node = document.getElementById('tab-' + id);
  //console.log('?', 'tab-' + id, new_tab_node);
  new_tab_node.classList.add('border-sky-500');
  new_tab_node.classList.remove('border-transparent');
  var old_tab = document.getElementById(cur);
  old_tab.style.display = 'none';
  var old_tab_node = document.getElementById('tab-' + cur )
  old_tab_node.classList.remove('border-sky-500')
  old_tab_node.classList.add('border-transparent');
  tabs[cmp_id] = id;
}

function filterByClass(cls, event) {
  const searchTerm = event.target.value.trim().toLowerCase();
  const searchable = document.getElementsByClassName(cls);
  Array.from(searchable).forEach(element => {
    element.style.display = 'block';
  });
  Array.from(searchable).forEach(element => {
    const hasMatch = element.textContent.toLowerCase().indexOf(searchTerm) >= 0
    element.style.display = hasMatch ? 'block' : 'none';
  });
}

function $(x) { return document.getElementById(x); }


document.addEventListener('keydown', (e) => {
    if(e.key === 'm' && (event.metaKey || e.ctrlKey))  {
        e.preventDefault();
        document.getElementById('main-menu').showModal();
    }
});

// TODO: move to uui
function lazyscript(glob, url, f) {
    // console.log('check', glob, window[glob])
    if(!window[glob]) {
        const script = document.createElement('script');
        script.src = url;
        document.head.appendChild(script);
        script.onload = ()=> { f()}
    } else {
        f()
    }
}

function copyCode(id, markid) {
    navigator.clipboard.writeText($(id).innerText)
    $(markid).classList.remove('hidden');
    setTimeout(()=> { $(markid).classList.add('hidden') }, 3000)
}
