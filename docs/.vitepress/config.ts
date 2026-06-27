import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'RxJS Deep Dive',
  description: 'Reactive Architecture, Operator Policies, and State Streams — a 16-module course',
  cleanUrls: true,

  themeConfig: {
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Modules', link: '/modules/00-setup' },
      { text: 'Appendices', link: '/appendices/a-anti-patterns' }
    ],

    sidebar: [
      {
        text: 'Course Modules',
        items: [
          { text: 'Module 0 — Setup and Mental Model', link: '/modules/00-setup' },
          { text: 'Module 1 — History and Lineage', link: '/modules/01-history' },
          { text: 'Module 2 — The Observable Contract', link: '/modules/02-observable-contract' },
          { text: 'Module 3 — Creation and Boundaries', link: '/modules/03-creation' },
          { text: 'Module 4 — Operators as Behavior Stories', link: '/modules/04-operators' },
          { text: 'Module 5 — Flattening Policies', link: '/modules/05-flattening' },
          { text: 'Module 6 — Time, Rate Limiting, and Schedulers', link: '/modules/06-time' },
          { text: 'Module 7 — Combining Streams', link: '/modules/07-combining' },
          { text: 'Module 8 — Error and Recovery Policies', link: '/modules/08-error' },
          { text: 'Module 9 — Hot, Cold, and Shared Streams', link: '/modules/09-hot-cold' },
          { text: 'Module 10 — State as a Stream', link: '/modules/10-state' },
          { text: 'Module 11 — TypeScript and Runtime Safety', link: '/modules/11-typescript' },
          { text: 'Module 12 — Custom Operators and DSL Design', link: '/modules/12-custom-operators' },
          { text: 'Module 13 — Testing with Virtual Time', link: '/modules/13-testing' },
          { text: 'Module 14 — Framework Integration', link: '/modules/14-framework' },
          { text: 'Module 15 — Capstone Project', link: '/modules/15-capstone' }
        ]
      },
      {
        text: 'Appendices',
        items: [
          { text: 'Appendix A — Common Anti-Patterns', link: '/appendices/a-anti-patterns' },
          { text: 'Appendix B — Real-World Patterns', link: '/appendices/b-real-world-patterns' },
          { text: 'Final Course Principles', link: '/appendices/final-principles' }
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/hansschenker/rxjs-deep-dive-course' }
    ],

    footer: {
      message: 'The domain can change. The RxJS machine stays the same.'
    },

    search: {
      provider: 'local'
    }
  }
})
