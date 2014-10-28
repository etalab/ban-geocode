var CSVToGeocode = function (options) {
    options = options || {};
    var holder = document.querySelector('#holder'),
        reader = new FileReader(), file,
        formData = new FormData(),
        availableColumns = document.querySelector('#availableColumns'),
        chosenColumns = document.querySelector('#chosenColumns'),
        browseLink = document.querySelector('#browseLink'),
        fileInput = document.querySelector('#fileInput'),
        matchAll = document.querySelector('input[name="matchAll"]'),
        submitButton = document.querySelector('#submitButton');

    var stop = function (e) {
        e.stopPropagation();
        e.preventDefault();
    };
    var submit = function () {
        var xhr = new XMLHttpRequest();
        xhr.open('POST', options.postURL || '.');
        xhr.overrideMimeType('text/csv; charset=utf-8');
        var columns = document.querySelectorAll('#chosenColumns li');
        for (var i = 0; i < columns.length; i++) {
            formData.append('columns', columns[i].id);
        }
        formData.append('match_all', !!matchAll.checked);
        xhr.onreadystatechange = function() {
            if (xhr.readyState === 4 && xhr.status === 200) {
                window.URL = window.URL || window.webkitURL;
                var blob = new Blob([xhr.responseText], {type: 'text/csv'});
                window.open(window.URL.createObjectURL(blob));
            }
        };
        xhr.send(formData);
        reader.readAsText(file);
    };

    var onFileDrop = function (e) {
        this.className = '';
        e.preventDefault();
        file = e.dataTransfer.files[0];
        reader.readAsText(file);
        return false;
    };
    var onDragOver = function (e) {
        stop(e);
        this.className = 'hover';
    };
    var onDragLeave = function (e) {
        stop(e);
        this.className = '';
        return false;
    };
    var onDragEnter = function (e) {
        stop(e);
    };
    var onFileLoad = function () {
        var rawHeaders = reader.result.slice(0, reader.result.indexOf('\n')),
            separators = [',', ';', '|', ':'], currentCount = 0, separator, count;
        for (var i = 0; i < separators.length; i++) {
          count = (rawHeaders.match(new RegExp('\\' + separators[i],'g')) || []).length;
          if (count > currentCount) {
              currentCount = count;
              separator = separators[i];
          }
        }
        if (currentCount === 0) return;
        var headers = rawHeaders.split(separator), column;
        availableColumns.innerHTML = '';
        chosenColumns.innerHTML = '';
        for (var j = 0; j < headers.length; j++) {
            column = document.createElement('li');
            column.setAttribute('draggable', 'true');
            column.innerHTML = column.value = column.id = headers[j];
            column.ondragstart = onColumnDragStart;
            column.onclick = onColumnClick;
            column.ondrop = onColumnDrop;
            column.ondragover = onColumnDragOver;
            column.ondragleave = onColumnDragLeave;
            availableColumns.appendChild(column);
        }
        submitButton.disabled = false;
        var blob = new Blob([reader.result], {type: 'text/csv'});
        formData.append('data', blob);
    };
    var onSubmit = function () {
      submit(file);
    };
    var onColumnDragStart = function (e) {
      e.dataTransfer.effectAllowed = 'copyMove';
      e.dataTransfer.setData('text/plain', this.id);
    };
    var onColumnDropboxDragover = function (e) {
        stop(e);
        this.className = 'hover';
        e.dataTransfer.dropEffect = 'copyMove';
    };
    var onColumnDropboxDragleave = function (e) {
        stop(e);
        this.className = '';
    };
    var onColumnDropboxDrop = function (e) {
        this.className = '';
        stop(e);
        var el = document.getElementById(e.dataTransfer.getData('text/plain'));
        el.parentNode.removeChild(el);
        chosenColumns.appendChild(el);
        return false;
    };
    var onColumnDrop = function (e) {
        stop(e);
        var el = document.getElementById(e.dataTransfer.getData('text/plain'));
        this.parentNode.insertBefore(el, this);
    };
    var onColumnClick = function (e) {
        this.className = '';
        var from, to;
        if (this.parentNode === chosenColumns) {
            from = chosenColumns;
            to = availableColumns;
        } else {
            from = availableColumns;
            to = chosenColumns;
        }
        from.removeChild(this);
        to.appendChild(this);
    };
    var onColumnDragOver = function (e) {
        this.className = 'hover';
    };
    var onColumnDragLeave = function (e) {
        this.className = '';
    };
    var onBrowseLinkClick = function (e) {
        stop(e);
        fileInput.click();
    };
    var onFileInputChange = function (e) {
        e.preventDefault();
        file = this.files[0];
        reader.readAsText(file);
        return false;
    };
    reader.addEventListener('load', onFileLoad, false);
    holder.addEventListener('dragenter', onDragEnter, false);
    holder.addEventListener('dragover', onDragOver, false);
    holder.addEventListener('dragleave', onDragLeave, false);
    holder.addEventListener('drop', onFileDrop, false);
    submitButton.addEventListener('click', onSubmit, false);
    chosenColumns.addEventListener('dragover', onColumnDropboxDragover, false);
    chosenColumns.addEventListener('dragleave', onColumnDropboxDragleave, false);
    chosenColumns.addEventListener('drop', onColumnDropboxDrop, false);
    browseLink.addEventListener('click', onBrowseLinkClick, false);
    fileInput.addEventListener('change', onFileInputChange, false);

};
