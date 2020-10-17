/*************************
 * 
 * HTML Contents
 * https://github.com/psalmody/html-contents
 * MIT License
 * 
 */

const htmlContents = function(toc, options) {
    options = Object.assign({}, {
    area: '',         // container with hadings e.g.: '.wrapper'
    top: 2,           // biggest header to include in outline - default H2
    bottom: 6,        // smallest header to include in outline - default H3
    addIds: true,     // addIds - yes by default to H* that don't have them
    addLinks: true,   // addLinks - 
    listType: 'u',    // 'u' or 'o' - (u)nordered or (o)rdered list type
    filter: false     // CSS style selector to exclude from outline
  }, options)

  //shared functions
  const getLevel = function(str) {
    return parseInt(str.replace(/[a-z,A-Z]/g,''))
  }
  //create listitem html
  const listItem = function(el, level) {
    let li = '<li data-level="' + level + '">'
    if (options.addLinks) li += '<a href="#' + el.id + '">'
    li += el.textContent
    if (options.addLinks) li += '</a>'
    li += '</li>'
    return li
  }

  //toc of contents - remove # if necessary
  let TOC = document.getElementById(toc.replace(/\#/g, ''))

  //get levels in between top and bottom - make query string
  let lvls = []
  for (let i = options.top; i <= options.bottom; i++) {
    lvls.push(options.area + ' h' + i);
  }

  //select all levels
  let hs = document.querySelectorAll(lvls.join(','))
  let headers

  //if filter?
  if (options.filter instanceof Function) {
    let arr = Array.prototype.slice.call(hs)
    headers = arr.filter(options.filter)
  } else if (options.filter instanceof String) {
    let arr = Array.prototype.slice.call(hs)
    headers = arr.filter(function(el) {
      return !el.matches(options.filter)
    })
  } else {
    headers = Array.prototype.slice.call(hs)
  }

  //add ids if necessary
  if (options.addIds) {
    //keep track of ids
    let ids = []
    headers.forEach(function(el) {
      //if it has an id already, just add that to the array
      if (el.id) return ids.push(el.id)
      //id will be the textcontent without non-letter characters in lower case
      let id = el.textContent.replace(/[^a-zA-Z]/g, '').toLowerCase()
      while(ids.indexOf(id) !== -1) {
        //add zs to the end until we have a unique id
        id = id + 'z'
      }
      //ready to be tracked
      ids.push(id)
      //okay we can set now
      el.id = id
    })
  }

  //make list
  //current level
  let prevLevel = options.top
  let html = '<' + options.listType + 'l>'
  headers.forEach(function(h) {
    let currentLevel = getLevel(h.tagName)
    let li = listItem(h, currentLevel)
    //if we're still at level
    if (currentLevel === prevLevel) {
      html += li
    } else if (currentLevel < prevLevel) {
      //if we've gone back up
      html += ('</' + options.listType + 'l>').repeat(prevLevel - currentLevel) + li
    } else {
      //we've gone down
      html += '<' + options.listType + 'l>' + li
    }
    prevLevel = currentLevel
  })
  TOC.insertAdjacentHTML('beforeend', html)
  
}

// add .active class to the selected TOC item
function handleActiveClass (selector) {
  var links = document.querySelectorAll(selector + ' a'); 
 
  for (var i = 0; i < links.length; i++) {
    links[i].addEventListener('click', function () {
      for (var j = 0; j < links.length; j++) {
        links[j].classList.remove('active');
      }
      this.classList.add('active');
    }, false);
  } 
  
  if (window.location.hash && window.location.hash !== '') { 
    var selectedLink = document.querySelector(selector + ' a[href$="' + window.location.hash + '"]');  
 
    if (selectedLink) {
      selectedLink.classList.add('active');
    }
  }
}