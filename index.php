<?php
// Basic configuration
$base_dir = __DIR__; // Current directory
$show_hidden = false; // Show hidden files
$allowed_extensions = []; // Leave empty to show all files

// Get current directory from URL parameter
$current_dir = isset($_GET['dir']) ? $_GET['dir'] : '';
$current_dir = str_replace(['../', '..\\'], '', $current_dir); // Security: prevent directory traversal
$full_path = realpath($base_dir . '/' . $current_dir);

// Security check: ensure we stay within base directory
if (!$full_path || strpos($full_path, realpath($base_dir)) !== 0) {
    $full_path = realpath($base_dir);
    $current_dir = '';
}

// Get directory contents
$items = scandir($full_path);
$directories = [];
$files = [];

foreach ($items as $item) {
    if ($item === '.' || ($item === '..' && $current_dir === '')) continue;
    if (!$show_hidden && $item[0] === '.') continue;
    
    $item_path = $full_path . '/' . $item;
    if (is_dir($item_path)) {
        $directories[] = $item;
    } else {
        $files[] = $item;
    }
}

// Sort items
sort($directories);
sort($files);
?>
<!DOCTYPE html>
<html>
<head>
    <title>File Navigator - <?= htmlspecialchars($current_dir ?: '/') ?></title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .nav-item { padding: 8px; border-bottom: 1px solid #eee; }
        .directory { background: #f0f8ff; font-weight: bold; }
        .file { background: #f9f9f9; }
        .nav-item:hover { background: #e6f3ff; }
        a { text-decoration: none; color: #333; }
        .up-link { margin-bottom: 20px; padding: 10px; background: #ddd; }
        .icon { margin-right: 8px; }
    </style>
</head>
<body>
    <h1>📁 File Navigator</h1>
    <p>Current Directory: <strong>/<?= htmlspecialchars($current_dir) ?></strong></p>
    
    <?php if ($current_dir): ?>
    <div class="up-link">
        <a href="?dir=<?= urlencode(dirname($current_dir) === '.' ? '' : dirname($current_dir)) ?>">
            <span class="icon">⬆️</span>Go Up
        </a>
    </div>
    <?php endif; ?>
    
    <div class="file-list">
        <?php foreach ($directories as $dir): ?>
        <div class="nav-item directory">
            <a href="?dir=<?= urlencode($current_dir ? $current_dir . '/' . $dir : $dir) ?>">
                <span class="icon">📁</span><?= htmlspecialchars($dir) ?>
            </a>
        </div>
        <?php endforeach; ?>
        
        <?php foreach ($files as $file): ?>
        <div class="nav-item file">
            <a href="<?= htmlspecialchars($current_dir ? $current_dir . '/' . $file : $file) ?>" target="_blank">
                <span class="icon">📄</span><?= htmlspecialchars($file) ?>
            </a>
            <small style="float: right; color: #666;">
                <?= date('Y-m-d H:i:s', filemtime($full_path . '/' . $file)) ?>
                (<?= formatBytes(filesize($full_path . '/' . $file)) ?>)
            </small>
        </div>
        <?php endforeach; ?>
    </div>
    
    <?php if (empty($directories) && empty($files)): ?>
    <p><em>Directory is empty</em></p>
    <?php endif; ?>
</body>
</html>

<?php
function formatBytes($size, $precision = 2) {
    $units = array('B', 'KB', 'MB', 'GB', 'TB');
    for ($i = 0; $size > 1024 && $i < count($units) - 1; $i++) {
        $size /= 1024;
    }
    return round($size, $precision) . ' ' . $units[$i];
}
?>
