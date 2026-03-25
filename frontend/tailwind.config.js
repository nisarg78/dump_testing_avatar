/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{html,ts}",
  ],
  theme: {
    extend: {
      colors: {
        // BMO Primary Blue Palette (Lochmara Blue)
        primary: {
          50: '#e6f4fb',
          100: '#cce9f7',
          200: '#99d3ef',
          300: '#66bde7',
          400: '#33a7df',
          500: '#0079C1',  // BMO Lochmara Blue (Primary)
          600: '#006aa8',
          700: '#005587',  // BMO Secondary/Dark Blue
          800: '#004a73',
          900: '#003d5a',
          950: '#002d42',
        },
        // BMO Secondary/Accent Palette
        secondary: {
          50: '#e6f7fc',
          100: '#cceff9',
          200: '#99dff3',
          300: '#66cfed',
          400: '#33bfe7',
          500: '#00a9e0',  // BMO Light Blue
          600: '#0098c9',
          700: '#0087b2',
          800: '#00769b',
          900: '#006584',
          950: '#00546d',
        },
        // BMO Brand Colors
        bmo: {
          blue: '#0079C1',        // Lochmara Blue (Primary)
          'blue-dark': '#005587', // Secondary/Dark Blue
          'blue-light': '#00a9e0',
          red: '#ED1C24',         // Crimson Red
          navy: '#002d72',
          green: '#00843d',
          gold: '#ffc72c',
        }
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'fade-in': 'fadeIn 0.3s ease-in-out',
        'slide-in': 'slideIn 0.3s ease-out',
      },
      keyframes: {
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        slideIn: {
          '0%': { transform: 'translateX(-10px)', opacity: '0' },
          '100%': { transform: 'translateX(0)', opacity: '1' },
        },
      },
    },
  },
  plugins: [],
}
