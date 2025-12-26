import { useState, useEffect, createContext, useContext } from 'react';

const OnboardingContext = createContext(null);

export function useOnboarding() {
    return useContext(OnboardingContext);
}

const TOUR_STEPS = [
    {
        id: 'welcome',
        target: null,
        title: 'Welcome to YXDB Converter! 🎉',
        content: 'This tool helps you convert Alteryx .yxdb files to SQL databases and query them using natural language.',
        placement: 'center'
    },
    {
        id: 'upload',
        target: '.upload-zone',
        title: 'Upload Your File',
        content: 'Drag and drop a .yxdb file here, or click to browse. You can also try the demo mode to see how it works.',
        placement: 'bottom'
    },
    {
        id: 'tabs',
        target: '.nav-tabs',
        title: 'Navigation',
        content: 'Use these tabs to switch between Upload, Schema, Query, History, and Dashboard views.',
        placement: 'bottom'
    },
    {
        id: 'schema',
        target: '.schema-viewer',
        title: 'Schema Viewer',
        content: 'After conversion, view your data structure here. You can download the SQLite database file.',
        placement: 'right'
    },
    {
        id: 'query',
        target: '.query-input-group',
        title: 'Natural Language Queries',
        content: 'Ask questions in plain English like "Show all records where sales > 1000" and AI will convert it to SQL.',
        placement: 'top'
    },
    {
        id: 'templates',
        target: '.sample-queries',
        title: 'Quick Templates',
        content: 'Click these buttons for common query patterns. They help you get started quickly.',
        placement: 'bottom'
    },
    {
        id: 'results',
        target: '.results-container',
        title: 'Query Results',
        content: 'Results appear here in a sortable, filterable table. Export to CSV or JSON with one click.',
        placement: 'top'
    },
    {
        id: 'done',
        target: null,
        title: "You're All Set! 🚀",
        content: "Now you're ready to convert and query your data. Press Ctrl+/ anytime to see keyboard shortcuts.",
        placement: 'center'
    }
];

export function OnboardingProvider({ children }) {
    const [isActive, setIsActive] = useState(false);
    const [currentStep, setCurrentStep] = useState(0);
    const [hasCompleted, setHasCompleted] = useState(() => {
        return localStorage.getItem('yxdb-onboarding-completed') === 'true';
    });

    useEffect(() => {
        // Auto-start onboarding for new users
        if (!hasCompleted && !isActive) {
            const timer = setTimeout(() => {
                setIsActive(true);
            }, 1000);
            return () => clearTimeout(timer);
        }
    }, [hasCompleted]);

    const startTour = () => {
        setCurrentStep(0);
        setIsActive(true);
    };

    const nextStep = () => {
        if (currentStep < TOUR_STEPS.length - 1) {
            setCurrentStep(prev => prev + 1);
        } else {
            completeTour();
        }
    };

    const prevStep = () => {
        if (currentStep > 0) {
            setCurrentStep(prev => prev - 1);
        }
    };

    const skipTour = () => {
        completeTour();
    };

    const completeTour = () => {
        setIsActive(false);
        setHasCompleted(true);
        localStorage.setItem('yxdb-onboarding-completed', 'true');
    };

    const resetTour = () => {
        localStorage.removeItem('yxdb-onboarding-completed');
        setHasCompleted(false);
        setCurrentStep(0);
    };

    return (
        <OnboardingContext.Provider value={{
            isActive,
            currentStep,
            totalSteps: TOUR_STEPS.length,
            step: TOUR_STEPS[currentStep],
            hasCompleted,
            startTour,
            nextStep,
            prevStep,
            skipTour,
            resetTour
        }}>
            {children}
            {isActive && <TourOverlay />}
        </OnboardingContext.Provider>
    );
}

function TourOverlay() {
    const { step, currentStep, totalSteps, nextStep, prevStep, skipTour } = useOnboarding();
    const [targetRect, setTargetRect] = useState(null);

    useEffect(() => {
        if (step.target) {
            const element = document.querySelector(step.target);
            if (element) {
                const rect = element.getBoundingClientRect();
                setTargetRect(rect);
                element.scrollIntoView({ behavior: 'smooth', block: 'center' });
            } else {
                setTargetRect(null);
            }
        } else {
            setTargetRect(null);
        }
    }, [step]);

    const tooltipStyle = {};
    if (targetRect && step.placement !== 'center') {
        switch (step.placement) {
            case 'top':
                tooltipStyle.top = targetRect.top - 16;
                tooltipStyle.left = targetRect.left + targetRect.width / 2;
                tooltipStyle.transform = 'translate(-50%, -100%)';
                break;
            case 'bottom':
                tooltipStyle.top = targetRect.bottom + 16;
                tooltipStyle.left = targetRect.left + targetRect.width / 2;
                tooltipStyle.transform = 'translateX(-50%)';
                break;
            case 'left':
                tooltipStyle.top = targetRect.top + targetRect.height / 2;
                tooltipStyle.left = targetRect.left - 16;
                tooltipStyle.transform = 'translate(-100%, -50%)';
                break;
            case 'right':
                tooltipStyle.top = targetRect.top + targetRect.height / 2;
                tooltipStyle.left = targetRect.right + 16;
                tooltipStyle.transform = 'translateY(-50%)';
                break;
        }
    }

    return (
        <div className="tour-overlay">
            {/* Spotlight */}
            {targetRect && (
                <div
                    className="tour-spotlight"
                    style={{
                        top: targetRect.top - 8,
                        left: targetRect.left - 8,
                        width: targetRect.width + 16,
                        height: targetRect.height + 16
                    }}
                />
            )}

            {/* Tooltip */}
            <div
                className={`tour-tooltip tour-${step.placement}`}
                style={step.placement === 'center' ? {} : tooltipStyle}
            >
                <div className="tour-header">
                    <h4 className="tour-title">{step.title}</h4>
                    <button className="tour-close" onClick={skipTour}>×</button>
                </div>

                <p className="tour-content">{step.content}</p>

                <div className="tour-footer">
                    <div className="tour-progress">
                        {currentStep + 1} / {totalSteps}
                    </div>
                    <div className="tour-actions">
                        {currentStep > 0 && (
                            <button className="btn btn-secondary btn-sm" onClick={prevStep}>
                                Back
                            </button>
                        )}
                        <button className="btn btn-primary btn-sm" onClick={nextStep}>
                            {currentStep === totalSteps - 1 ? 'Finish' : 'Next'}
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}

export default OnboardingProvider;
