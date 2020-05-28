load("easy.out");
out = easy;
load("easy.normal");
ans = easy;
err = abs(out - ans) ./ (ans + 1e-5);
err = min(err, 2);
xbins = 0 : 0.1 : 2;
figure;
hist(err, xbins);

load("middle.out");
out = middle;
load("middle.normal");
ans = middle;
err = abs(out - ans) ./ (ans + 1e-5);
err = min(err, 2);
xbins = 0 : 0.1 : 2;
figure;
hist(err, xbins);

load("hard.out");
out = hard;
load("hard.normal");
ans = hard;
err = abs(out - ans) ./ (ans + 1e-5);
err = min(err, 2);
xbins = 0 : 0.1 : 2;
figure;
hist(err, xbins);