import { defineConfig, type UserConfig } from 'vitepress';
import { withMermaid } from 'vitepress-plugin-mermaid';

// https://vitepress.dev/reference/site-config
const config: UserConfig = {
	title: 'maestro',
	description:
		'Atomic in-memory state replication for Go, from one writer to every replica',
	lang: "en-US",
	lastUpdated: true,
	appearance: "dark",
	ignoreDeadLinks: false,
	base: '/maestro/',
	vite: {
		optimizeDeps: {
			exclude: ['mermaid'],
		}
	},
	sitemap: {
		hostname: 'https://foomo.github.io/maestro',
	},
	themeConfig: {
		// https://vitepress.dev/reference/default-theme-config
		logo: '/logo.png',
		outline: [2, 4],
		nav: [
			{ text: 'Guide', link: '/guide/introduction' },
			{ text: 'Reference', link: '/reference/' },
		],
		sidebar: [
			{
				text: 'Guide',
				items: [
					{ text: 'Introduction', link: '/guide/introduction' },
					{ text: 'Getting Started', link: '/guide/getting-started' },
					{ text: 'The 3PC Protocol', link: '/guide/core-concepts' },
					{ text: 'Implementing StageHandler', link: '/guide/stagehandler' },
					{ text: 'BlobStore', link: '/guide/blobstore' },
					{ text: 'keel Integration', link: '/guide/keel-integration' },
				],
			},
			{
				text: 'Reference',
				items: [
					{ text: 'Configuration', link: '/reference/' },
				],
			},
			{
				text: 'Contributing',
				collapsed: true,
				items: [
					{
						text: "Guideline",
						link: '/CONTRIBUTING.md',
					},
					{
						text: "Code of conduct",
						link: '/CODE_OF_CONDUCT.md',
					},
					{
						text: "Security guidelines",
						link: '/SECURITY.md',
					},
				],
			},
		],
		socialLinks: [
			{ icon: 'github', link: 'https://github.com/foomo/maestro' },
		],
		editLink: {
			pattern: 'https://github.com/foomo/maestro/edit/main/docs/:path',
		},
		search: {
			provider: 'local',
		},
		footer: {
			message: 'Made with ♥ <a href="https://www.foomo.org">foomo</a> by <a href="https://www.bestbytes.com">bestbytes</a>',
		},
	},
	markdown: {
		// https://github.com/vuejs/vitepress/discussions/3724
		theme: {
			light: 'catppuccin-latte',
			dark: 'catppuccin-frappe',
		}
	},
	head: [
		['meta', { name: 'theme-color', content: '#ffffff' }],
		['link', { rel: 'icon', href: '/logo.png' }],
		['meta', { name: 'author', content: 'foomo by bestbytes' }],
		// OpenGraph
		['meta', { property: 'og:title', content: 'foomo/maestro' }],
		[
			'meta',
			{
				property: 'og:image',
				content: 'https://github.com/foomo/maestro/blob/main/docs/public/banner.png?raw=true',
			},
		],
		[
			'meta',
			{
				property: 'og:description',
				content:
					'One soloist writes the score. Every player turns the page together. Atomic in-memory state replication for Go.',
			},
		],
		['meta', { name: 'twitter:card', content: 'summary_large_image' }],
		[
			'meta',
			{
				name: 'twitter:image',
				content: 'https://github.com/foomo/maestro/blob/main/docs/public/banner.png?raw=true',
			},
		],
		[
			'meta', { name: 'viewport', content: 'width=device-width, initial-scale=1.0, viewport-fit=cover',
			},
		],
	]
}

export default withMermaid(defineConfig(config));
