/**
 * 南昌大学附属眼科医院 — 尚蕾 个人主页
 * 通用 JavaScript
 */

(function () {
    'use strict';

    // ===== 移动端导航切换 =====
    var navToggle = document.querySelector('.nav-toggle');
    var navLinks = document.querySelector('.nav-links');

    if (navToggle && navLinks) {
        navToggle.addEventListener('click', function () {
            navLinks.classList.toggle('open');
        });
        // 点击导航链接后关闭
        navLinks.querySelectorAll('a').forEach(function (link) {
            link.addEventListener('click', function () {
                navLinks.classList.remove('open');
            });
        });
    }

    // ===== 导航栏滚动效果 =====
    var topNav = document.querySelector('.top-nav');
    if (topNav) {
        window.addEventListener('scroll', function () {
            if (window.scrollY > 20) {
                topNav.classList.add('scrolled');
            } else {
                topNav.classList.remove('scrolled');
            }
        });
    }

    // ===== 当前页面高亮导航 =====
    var currentPage = window.location.pathname.split('/').pop() || 'index.html';
    var navAnchors = document.querySelectorAll('.nav-links a');
    navAnchors.forEach(function (anchor) {
        var href = anchor.getAttribute('href');
        if (href === currentPage || (currentPage === 'index.html' && href === 'index.html')) {
            anchor.classList.add('active');
        } else {
            anchor.classList.remove('active');
        }
    });

    // ===== 平滑滚动 (首页锚点) =====
    document.querySelectorAll('a[href^="#"]').forEach(function (anchor) {
        anchor.addEventListener('click', function (e) {
            var targetId = this.getAttribute('href').substring(1);
            var target = document.getElementById(targetId);
            if (target) {
                e.preventDefault();
                target.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
        });
    });

})();
