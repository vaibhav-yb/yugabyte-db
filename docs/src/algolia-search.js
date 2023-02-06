import algoliasearch from 'algoliasearch';

(function () {
  'use strict';

  const ignoreClickOnMeElement = document.querySelector('body:not(.td-searchpage) .search-area');
  const searchInput = document.getElementById('search-query');

  /**
   * Provided name value from the Query param either from URL or with the passed
   * search query.
   */
  function searchURLParameter(name, fetchFrom) {
    if (!fetchFrom) {
      fetchFrom = location.search;
    }

    return decodeURIComponent((new RegExp('[?|&]' + name + '=([^&;]+?)(&|#|;|$)').exec(fetchFrom) || [null, ''])[1].replace(/\+/g, '%20')) || null;
  }

  /**
   * Create a URL that contains query, tab, items etc.
   */
  function createSearchUrl(pager = 1) {
    const searchText = searchInput.value.trim();
    if (history.pushState) {
      let addQueryParams = '';
      if (searchText && searchText.trim().length > 0) {
        document.getElementById('search-query').classList.add('have-text');
        addQueryParams = '?query=' + searchText.trim();
      } else {
        addQueryParams = '/search/';
        document.getElementById('search-query').classList.remove('have-text');
      }
      if (document.querySelector('body').classList.contains('td-searchpage')) {
        if (pager > 1) {
          addQueryParams += '&page=' + pager;
        }
        window.history.pushState({}, '', addQueryParams);
      }
    }
  }

  /**
   * Show Message when Search is empty.
   */
  function emptySearch() {
    setTimeout(() => {
      if (document.querySelector('body').classList.contains('td-searchpage--')) {
        document.querySelector('#doc-hit').innerHTML = '<li class="no-result">Search data related to <b>YugabyteDb</b> in our Documentation </li>';
        document.querySelector('#pagination-docs').style.display = 'none';
      } else {
        document.querySelector('.search-result').style.display = 'none';
      }
    }, 800);
  }

  /**
   * Main Docs section HTML.
   */
  function docsSection(hitIs) {
    console.log(hitIs);
    let content = '';
    hitIs.forEach(hit => {
      const hitLevel0 = hit.hierarchy.lvl0;
      if (!hitLevel0) {
        return;
      }

      content += '<li>' +
        '<div class="search-title">' +
        '<a href="' + hit.url.replace(/^(?:\/\/|[^/]+)*\//, '/') + '">' +
        '<span class="search-title-inner">' + hitLevel0 + '</span>' +
        '<div class="breadcrumb-item">' + hit.breadcrumb + '</div>' +
        '</a>' +
        '</div>' +
        '</li>';
    });

    return content;
  }

  /**
   * Search Pagination HTML.
   */
  function searchPagination(pagerInfo) {
    document.querySelector('#docs').removeAttribute('style');

    const searchpager = document.getElementById(pagerInfo.pagerId);
    const currentPage = pagerInfo.currentPage;
    const totalPages = pagerInfo.totalPages;

    let firstPage = '<span class="pager-btn" data-pager="' + 1 + '">1</span>';
    let lastPage = '<span class="pager-btn" data-pager="' + totalPages + '">' + totalPages + '</span>';
    let nextPageHTML = '<span class="right-btn pager-btn" data-pager="' + (currentPage + 1) + '"> &raquo; </span>';
    let pagerClick = '';
    let pagerClickLength = 0;
    let pagerLoop = 0;
    let pagerFull = '';
    let prevPageHTML = '<span class="left-btn pager-btn" data-pager="' + (currentPage - 1) + '"> &laquo; </span>';

    if (currentPage === 1) {
      prevPageHTML = '';
      firstPage = '<span class="pager-btn active-page" data-pager="' + 1 + '">1</span>';
    }

    if (currentPage === totalPages) {
      nextPageHTML = '';
      lastPage = '<span class="pager-btn active-page" data-pager="' + totalPages + '">' + totalPages + '</span>';
    }

    if (totalPages > 1) {
      let pagerStep = 2;
      let loopTill = totalPages - 1;

      pagerFull = firstPage;

      if (currentPage >= 4) {
        pagerStep = currentPage - 2;
      }

      if (currentPage !== totalPages) {
        if (currentPage === 1 || currentPage === 2 || currentPage === 3) {
          if (totalPages > 6 && currentPage === 1) {
            loopTill = currentPage + 5;
          } else if (totalPages > 6 && currentPage === 2) {
            loopTill = currentPage + 4;
          } else if (totalPages > 6 && currentPage === 3) {
            loopTill = currentPage + 3;
          }
        } else if ((currentPage + 2) < totalPages) {
          loopTill = currentPage + 2;
        } else if ((currentPage + 1) < totalPages) {
          loopTill = currentPage + 1;
        }
      }

      if (pagerStep > 2) {
        pagerFull += '<span class="dots-3">...</span>';
      }

      for (pagerStep; pagerStep <= loopTill; pagerStep += 1) {
        let activeClass = '';
        if (pagerStep === currentPage) {
          activeClass = ' active-page';
        }

        pagerFull += '<span class="pager-btn' + activeClass + '" data-pager="' + pagerStep + '">' + pagerStep + '</span>';
      }

      if (loopTill < (totalPages - 1)) {
        pagerFull += '<span class="dots-3">...</span>';
      }

      pagerFull += lastPage;
    }

    searchpager.innerHTML = '<nav>' +
      '<div class="pager-area">' +
      '<div class="left-right-button" data-pager-type="' + pagerInfo.pagerType + '">' +
      prevPageHTML +
      '<span class="page-number">' +
      pagerFull +
      '</span>' +
      nextPageHTML +
      '</div>' +
      '</div>' +
      '</nav>' +
      '<span class="left-right-button mobile-button" data-pager-type="' + pagerInfo.pagerType + '"></span>';

    pagerClick = document.querySelectorAll('#' + pagerInfo.pagerId + ' .left-right-button .pager-btn');
    pagerClickLength = pagerClick.length;

    while (pagerLoop < pagerClickLength) {
      pagerClick[pagerLoop].addEventListener('click', searchSettings);
      pagerLoop += 1;
    }
  }

  /**
   * Add queries with filters selected by user and call search algolia function.
   */
  function searchAlgolia() {
    const searchValue = searchInput.value.trim();
    if (searchValue.length > 0) {
      document.querySelector('.search-result').style.display = 'block';
    } else {
      emptySearch();
      return;
    }

    let perPageCount = 8;
    if (document.querySelector('body').classList.contains('td-searchpage')) {
      perPageCount = 10;
    }

    const client = algoliasearch('UMBCUJCBE8', 'a879f0ab89b677264d0c6e087b714fd8');
    const index = client.initIndex('yugabyte_docs');
    const pageItems = searchURLParameter('page');
    const searchpagerparent = document.querySelector('#pagination-docs');
    const searchOptions = {
      hitsPerPage: perPageCount,
      page: 0,
      filters: 'version:"preview"',
    };

    if (pageItems && pageItems > 0) {
      searchOptions.page = pageItems - 1;
    }

    index.search(searchValue, searchOptions).then(({hits, nbHits, nbPages, page}) => {
      let pagerDetails = {};
      let sectionHTML = '';
      sectionHTML += docsSection(hits);
      if (hits.length > 0) {
        document.getElementById('doc-hit').innerHTML = sectionHTML;
      } else {
        document.getElementById('doc-hit').innerHTML = '<li class="no-result">0 results found for <b>"' + searchValue + '"</b></li>';
      }

      if (document.querySelector('body').classList.contains('td-searchpage')) {
        pagerDetails = {
          currentPage: page + 1,
          pagerId: 'pagination-docs',
          pagerType: 'docs',
          totalHits: nbHits,
          totalPages: nbPages,
        };

        searchPagination(pagerDetails);
        searchpagerparent.className = 'pager results-' + nbHits;
      } else {
        searchpagerparent.className = 'pager results-' + nbHits;
        let viewAll = '';
        if (nbPages > 1) {
          viewAll = '<a href="/search/?query=' + searchValue + '" title="View all results">View all results</a>';
        }

        document.getElementById('pagination-docs').innerHTML = '<nav class="pager-area">' +
          '<div class="pager-area">' +
          '<span class="total-result">' + nbHits + ' Results</span>' +
          '</div>' +
          viewAll +
          '</nav>';
      }
    });
  }

  /**
   * Runs on keyup or filters click or Pagination click function.
   */
  function searchSettings(e) {
    if (e.type === 'click') {
      if (e.target.classList.contains('pager-btn') && !e.target.classList.contains('disabled')) {
        if (e.target.getAttribute('data-pager')) {
          if (document.querySelector('body').classList.contains('td-searchpage')) {
            createSearchUrl(e.target.getAttribute('data-pager'));
          }
          searchAlgolia();
          setTimeout(() => {
            window.scrollTo(0, 0);
          }, 800);
        }
      }
    } else {
      createSearchUrl();
      searchAlgolia();
    }
  }

  /**
   * This function attached all the events which is needed for the search to work
   * properly.
   */
  function addSearchEvents() {
    if (searchURLParameter('query') && document.querySelector('body').classList.contains('td-searchpage')) {
      searchInput.setAttribute('value', searchURLParameter('query'));
    }

    document.addEventListener('DOMContentLoaded', () => {
      searchAlgolia();
    });

    // Events binding.
    searchInput.addEventListener('keyup', searchSettings);
    searchInput.addEventListener('paste', searchSettings);
  }

  addSearchEvents();

  document.addEventListener('keydown', (event) => {
    if (event.code === 'Slash' || event.code === 'NumpadDivide') {
      document.querySelector('body').classList.add('search-focus');
      if (searchURLParameter('query')) {
        searchInput.setAttribute('value', searchURLParameter('query'));
      } else {
        searchInput.setAttribute('value', '');
      }

      setTimeout(() => {
        searchInput.focus();
        searchInput.select();
      }, 200);
    }
  });

  if (ignoreClickOnMeElement) {
    document.addEventListener('click', (event) => {
      const isClickInsideElement = ignoreClickOnMeElement.contains(event.target);
      if (!isClickInsideElement) {
        emptySearch();
      }
    });
  }
  const inputReset = document.querySelector('.reset-input');
  inputReset.addEventListener('click', () => {
    document.querySelector('.td-search-input').value = '';
    emptySearch();
    document.querySelector('.td-search-input').classList.remove('have-text');
    if (document.querySelector('body').classList.contains('td-searchpage')) {
      window.history.pushState({}, '', '/search/');
    }
  });
})();

